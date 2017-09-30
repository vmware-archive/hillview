/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.*;
import org.hillview.dataset.*;
import org.hillview.utils.*;
import rx.Observable;
import rx.Observer;
import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.BiFunction;

/**
 * An RPC target is an object that has methods that are invoked from the UI
 * through the web server.  All these methods are tagged with @HillviewRpc.
 * When these objects are serialized as JSON only their String id is send; the
 * objects always reside on the web server.  They are managed by the RpcObjectManager.
 */
abstract class RpcTarget implements IJson {
    public final String objectId;
    /**
     * Computation that has generated this object.  Can only
     * be null for the initial object.
     */
    @Nullable
    public final HillviewComputation computation;

    /**
     * This constructor is only called for the InitialObject.
     */
    RpcTarget() {
        this.objectId = RpcObjectManager.initialObjectId;
        this.computation = null;
    }

    RpcTarget(HillviewComputation computation) {
        this.computation = computation;
        this.objectId = computation.resultId;
    }

    /**
     * Insert object in object manager maps.
     * Also, notify computations that may be waiting for object to appear.
     * This method should be called last thing after the construction of the
     * object has been completed.
     */
    void registerObject() {
        RpcObjectManager.instance.addObject(this);
        if (this.computation != null)
            this.computation.objectCreated(this);
    }

    private synchronized void saveSubscription(RpcRequestContext context, Subscription sub) {
        if (context.session != null)
            RpcObjectManager.instance.addSubscription(context.session, sub);
    }

    /**
     * Use reflection to fina a method with a given name that has an @HillviewRpc annotation.
     * All these methods should have the following signature:
     * method(RpcRequest req, RpcRequestContext context).
     */
    @Nullable
    private Method getMethod(String method) {
        Class<?> type = this.getClass();
        for (Method m : type.getDeclaredMethods()) {
            if (m.getName().equals(method) &&
                    m.isAnnotationPresent(HillviewRpc.class))
                return m;
        }
        return null;
    }

    /**
     * Dispatches an RPC request for execution.
     * This will look up the method in the RpcRequest using reflection
     * and invoke it using Java reflection.
     * The method that is being invoked is responsible for:
     * - parsing the arguments of the RpcCall
     * - sending the replies, in any number they may be, using the context
     * - closing the context session on termination.
     */
    void execute(RpcRequest request, RpcRequestContext context) {
        /*
        HillviewComputation computation = new HillviewComputation(this, request);
        String resultId = RpcObjectManager.instance.checkCache(computation);
        if (resultId != null) {
            RpcObjectManager.instance.getObject(resultId);
        }
        TODO: check if computation has happened and just send result back.
        */
        try {
            Method method = this.getMethod(request.method);
            if (method == null)
                throw new RuntimeException(this.toString() + ": No such method " + request.method);
            HillviewLogging.logger().info("Executing {}", request);
            method.invoke(this, request, context);
        } catch (Exception ex) {
            HillviewLogging.logger().error("Exception while invoking method on {}", this, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int hashCode() {
        return Converters.checkNull(this.objectId).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        RpcTarget rpcTarget = (RpcTarget) o;
        return this.objectId.equals(rpcTarget.objectId);
    }

    abstract class ResultObserver<T> implements Observer<PartialResult<T>> {
        final RpcRequest request;
        final RpcRequestContext context;
        final String name;
        final RpcTarget target;

        ResultObserver(String name, RpcRequest request, RpcTarget target,
                       RpcRequestContext context) {
            this.name = name;
            this.request = request;
            this.context = context;
            this.target = target;
        }

        @Override
        public void onCompleted() {
            HillviewLogging.logger().info("Computation completed for {}", this.name);
            this.request.syncCloseSession(this.context.session);
        }

        @Override
        public void onError(Throwable throwable) {
            if (this.context.session == null || !this.context.session.isOpen()) return;

            HillviewLogging.logger().error("{} onError", this.name);
            HillviewLogging.logger().error(throwable.toString());
            RpcReply reply = this.request.createReply(throwable);
            reply.send(this.context.session);
        }

        HillviewComputation getComputation() {
            if (this.context.computation != null)
                return this.context.computation;
            return new HillviewComputation(this.target, this.request);
        }
    }

    class SketchResultObserver<R extends IJson> extends ResultObserver<R> {
        SketchResultObserver(String name, RpcTarget target, RpcRequest request,
                             RpcRequestContext context) {
            super(name, request, target, context);
        }

        @Override
        public void onNext(PartialResult<R> pr) {
            HillviewLogging.logger().info("Received partial result from {}", this.name);
            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;

            RpcReply reply = this.request.createReply(Utilities.toJsonTree(pr));
            reply.send(session);
        }
    }

    /**
     * This observes a sketch computation, but only sends the final sketch result
     * to the consumer.  It performs aggregation by itself.
     * @param <R> Type of data.
     */
    class CompleteSketchResultObserver<T, R, S extends IJson> extends ResultObserver<R> {
        @Nullable
        private R last;
        private final BiFunction<R, HillviewComputation, S> postprocessing;

        CompleteSketchResultObserver(String name, RpcTarget target, RpcRequest request,
                                     RpcRequestContext context,
                                     BiFunction<R, HillviewComputation, S> postprocessing) {
            super(name, request, target, context);
            this.last = null;
            this.postprocessing = postprocessing;
        }

        @Override
        public void onNext(PartialResult<R> pr) {
            HillviewLogging.logger().info("Received partial result from {}", this.name);
            this.last = pr.deltaValue;
            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            // always send null data for partial results
            json.add("data", null);
            RpcReply reply = this.request.createReply(json);
            reply.send(session);
        }

        @Override
        public void onCompleted() {
            HillviewLogging.logger().info("Computation completed for {}", this.name);
            JsonObject json = new JsonObject();
            json.addProperty("done", 1.0);
            S result = this.postprocessing.apply(this.last, this.getComputation());

            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;
            json.add("data", result.toJsonTree());
            RpcReply reply = this.request.createReply(json);
            reply.send(this.context.session);
            this.request.syncCloseSession(this.context.session);
        }
    }

    class MapResultObserver<T> extends ResultObserver<IDataSet<T>> {
        @Nullable
        IDataSet<T> result;
        final BiFunction<IDataSet<T>, HillviewComputation, RpcTarget> factory;

        MapResultObserver(String name, RpcTarget target, RpcRequest request,
                          RpcRequestContext context,
                          BiFunction<IDataSet<T>, HillviewComputation, RpcTarget> factory) {
            super(name, request, target, context);
            this.factory = factory;
        }

        @Override
        public void onNext(PartialResult<IDataSet<T>> pr) {
            HillviewLogging.logger().info("Received partial result from ", this.name);

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            IDataSet<T> dataSet = pr.deltaValue;
            // Replace the "data" with the remote object ID
            if (dataSet != null) {
                this.result = dataSet;
                RpcTarget target = this.factory.apply(this.result, this.getComputation());
                json.addProperty("data", target.objectId);
            } else {
                json.add("data", null);
            }

            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;
            RpcReply reply = this.request.createReply(json);
            reply.send(session);
        }
    }

    @Override
    public String toString() {
        return "id: " + this.objectId;
    }

    /**
     * Default serialization of an RpcTarget is the object id.
     */
    @Override
    public JsonElement toJsonTree() {
        return IJson.gsonInstance.toJsonTree(this.objectId);
    }

    /**
     * Runs a sketch and sends the data received directly to the client.
     * @param data    Dataset to run the sketch on.
     * @param sketch  Sketch to run.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    <T, R extends IJson> void
    runSketch(IDataSet<T> data, ISketch<T, R> sketch,
              RpcRequest request, RpcRequestContext context) {
        // Run the sketch
        Observable<PartialResult<R>> sketches = data.sketch(sketch);
        // Knows how to add partial results
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        // Prefix sum of the partial results
        Observable<PartialResult<R>> add = sketches.scan(prm::add);
        // Send the partial results back
        SketchResultObserver<R> robs = new SketchResultObserver<R>(
                sketch.asString(), this, request, context);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(context, sub);
    }

    /**
     * Runs a sketch and sends the complete sketch result received directly to the client.
     * Progress updates are sent to the client, but accompanied by null values.
     * @param data    Dataset to run the sketch on.
     * @param sketch  Sketch to run.
     * @param postprocessing  This function is applied to the sketch results.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    <T, R, S extends IJson> void
    runCompleteSketch(IDataSet<T> data, ISketch<T, R> sketch,
                      BiFunction<R, HillviewComputation, S> postprocessing,
                      RpcRequest request, RpcRequestContext context) {
        // Run the sketch
        Observable<PartialResult<R>> sketches = data.sketch(sketch);
        // Knows how to add partial results
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        // Prefix sum of the partial results
        Observable<PartialResult<R>> add = sketches.scan(prm::add);
        // Send the partial results back
        CompleteSketchResultObserver<T, R, S> robs = new CompleteSketchResultObserver<T, R, S>(
                sketch.asString(), this, request, context, postprocessing);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(context, sub);
    }

    /**
     * Runs a map and sends the result directly to the client.
     * @param data    Dataset to run the map on.
     * @param map     Map to execute.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param context Context for the computation.
     */
    <T, S> void
    runMap(IDataSet<T> data, IMap<T, S> map,
           BiFunction<IDataSet<S>, HillviewComputation, RpcTarget> factory,
           RpcRequest request, RpcRequestContext context) {
        // Run the map
        Observable<PartialResult<IDataSet<S>>> stream = data.map(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                map.asString(), this, request, context, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(context, sub);
    }

    /**
     * Runs a flatmap and sends the result directly to the client.
     * @param data    Dataset to run the map on.
     * @param map     Map to execute.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param context Context for the computation.
     */
    <T, S> void
    runFlatMap(IDataSet<T> data, IMap<T, List<S>> map,
               BiFunction<IDataSet<S>, HillviewComputation, RpcTarget> factory,
               RpcRequest request, RpcRequestContext context) {
        // Run the flatMap
        Observable<PartialResult<IDataSet<S>>> stream = data.flatMap(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                map.asString(), this, request, context, factory);
        HillviewLogging.logger().info("Subscribing to flatMap");
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(context, sub);
    }

    /**
     * Runs a zip between two datasets.
     * @param data    Left dataset.
     * @param other   Right dataset.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param context Context for the computation.
     */
    <T, S> void
    runZip(IDataSet<T> data, IDataSet<S> other,
           BiFunction<IDataSet<Pair<T, S>>, HillviewComputation, RpcTarget> factory,
           RpcRequest request, RpcRequestContext context) {
        Observable<PartialResult<IDataSet<Pair<T, S>>>> stream = data.zip(other);
        PRDataSetMonoid<Pair<T, S>> monoid = new PRDataSetMonoid<Pair<T, S>>();
        Observable<PartialResult<IDataSet<Pair<T, S>>>> add = stream.scan(monoid::add);
        // We can actually reuse the MapResultObserver
        MapResultObserver<Pair<T, S>> robs = new MapResultObserver<Pair<T, S>>(
                                "zip", this, request, context, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(context, sub);
    }

    /**
     * Runs a management command and sends the data received directly to the client.
     * @param data    Dataset to run the manage command on.
     * @param command Command to run.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    <T, R extends IJson> void
    runManage(IDataSet<T> data, ControlMessage command,
              RpcRequest request, RpcRequestContext context) {
        // Run the sketch
        Observable<PartialResult<JsonList<ControlMessage.Status>>> sketches = data.manage(command);
        // Knows how to add partial results
        PartialResultMonoid<JsonList<ControlMessage.Status>> prm =
                new PartialResultMonoid<JsonList<ControlMessage.Status>>(
                        new JsonListMonoid<ControlMessage.Status>());
        // Prefix sum of the partial results
        Observable<PartialResult<JsonList<ControlMessage.Status>>> add = sketches.scan(prm::add);
        // Send the partial results back
        SketchResultObserver<JsonList<ControlMessage.Status>> robs =
                new SketchResultObserver<JsonList<ControlMessage.Status>>(
                    command.toString(), this, request, context);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(context, sub);
    }
}

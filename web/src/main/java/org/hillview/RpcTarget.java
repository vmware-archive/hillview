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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.BiFunction;

abstract class RpcTarget implements IJson {
    @Nullable // This is null for a very brief time
    String objectId;
    /**
     * Computation that has generated this object.  Can only
     * be null for the initial object.
     */
    @Nullable
    public final HillviewComputation computation;

    /**
     * This constructor is only called for the InitialObject.
     */
    protected RpcTarget() {
        this.objectId = RpcObjectManager.initialObjectId;
        this.computation = null;
        RpcObjectManager.instance.addObject(this);
    }

    /**
     * This constructor is called only when we know the id of the resulting object.
     * This usually happens when replaying a computation.
     * @param id           Id of the resulting object.
     * @param computation  computation that generated this object.
     */
    protected RpcTarget(String id, HillviewComputation computation) {
        this.objectId = id;
        this.computation = computation;
        RpcObjectManager.instance.addObject(this);
    }

    RpcTarget(HillviewComputation computation) {
        this.computation = computation;
        RpcObjectManager.instance.addObject(this);
    }

    public void setId(String objectId) {
        this.objectId = objectId;
    }

    private synchronized void saveSubscription(@Nullable Session session, Subscription sub) {
        if (session != null)
            RpcObjectManager.instance.addSubscription(session, sub);
    }

    /**
     * Use reflection to fina a method with a given name that has an @HillviewRpc annotation.
     * All these methods should have the following signature:
     * method(RpcRequest req, Session session).
     * The method is responsible for:
     * - parsing the arguments of the RpcCall
     * - sending the replies, in any number they may be, using the session
     * - closing the session on termination.
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
     */
    void execute(RpcRequest request, @Nullable Session session)
            throws InvocationTargetException, IllegalAccessException {
        /*
        HillviewComputation computation = new HillviewComputation(this, request);
        String resultId = RpcObjectManager.instance.checkCache(computation);
        if (resultId != null) {
            RpcObjectManager.instance.getObject(resultId);
        }
        TODO: check if computation has happened and just send result back.
        */

        Method method = this.getMethod(request.method);
        if (method == null)
            throw new RuntimeException(this.toString() + ": No such method " + request.method);
        method.invoke(this, request, session);
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
        return this.objectId != null ? this.objectId.equals(rpcTarget.objectId) : rpcTarget.objectId == null;
    }

    abstract class ResultObserver<T> implements Observer<PartialResult<T>> {
        final RpcRequest request;
        /**
         * The session can be null when requests are replayed without a client.
         */
        @Nullable
        final Session session;
        final String name;
        final RpcTarget target;

        ResultObserver(String name, RpcRequest request,
                       RpcTarget target, @Nullable Session session) {
            this.name = name;
            this.request = request;
            this.session = session;
            this.target = target;
        }

        @Override
        public void onCompleted() {
            HillviewLogging.logger().info("Computation completed for {}", this.name);
            this.request.syncCloseSession(this.session);
        }

        @Override
        public void onError(Throwable throwable) {
            if (this.session == null || !this.session.isOpen()) return;

            HillviewLogging.logger().error("{} onError", this.name);
            HillviewLogging.logger().error(throwable.toString());
            RpcReply reply = this.request.createReply(throwable);
            reply.send(this.session);
        }

        HillviewComputation getComputation() {
            return new HillviewComputation(this.target, this.request);
        }
    }

    class SketchResultObserver<R extends IJson> extends ResultObserver<R> {
        SketchResultObserver(String name, RpcTarget target, RpcRequest request,
                             @Nullable Session session) {
            super(name, request, target, session);
        }

        @Override
        public void onNext(PartialResult<R> pr) {
            HillviewLogging.logger().info("Received partial result from {}", this.name);
            if (this.session == null)
                return;
            if (!this.session.isOpen()) {
                HillviewLogging.logger().warn("Session closed, ignoring partial result");
                return;
            }

            RpcReply reply = this.request.createReply(Utilities.toJsonTree(pr));
            reply.send(this.session);
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
                                     @Nullable Session session,
                                     BiFunction<R, HillviewComputation, S> postprocessing) {
            super(name, request, target, session);
            this.last = null;
            this.postprocessing = postprocessing;
        }

        @Override
        public void onNext(PartialResult<R> pr) {
            HillviewLogging.logger().info("Received partial result from {}", this.name);
            this.last = pr.deltaValue;
            if (this.session == null)
                return;
            if (!this.session.isOpen()) {
                HillviewLogging.logger().warn("Session closed, ignoring partial result");
                return;
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            // always send null data for partial results
            json.add("data", null);
            RpcReply reply = this.request.createReply(json);
            reply.send(this.session);
        }

        @Override
        public void onCompleted() {
            HillviewLogging.logger().info("Computation completed for {}", this.name);
            JsonObject json = new JsonObject();
            json.addProperty("done", 1.0);
            S result = this.postprocessing.apply(this.last, this.getComputation());
            json.add("data", result.toJsonTree());
            RpcReply reply = this.request.createReply(json);
            reply.send(this.session);

            this.request.syncCloseSession(this.session);
        }
    }

    class MapResultObserver<T> extends ResultObserver<IDataSet<T>> {
        @Nullable
        IDataSet<T> result;
        final BiFunction<IDataSet<T>, HillviewComputation, RpcTarget> factory;

        MapResultObserver(String name, RpcTarget target, RpcRequest request, Session session,
                          BiFunction<IDataSet<T>, HillviewComputation, RpcTarget> factory) {
            super(name, request, target, session);
            this.factory = factory;
        }

        @Override
        public void onNext(PartialResult<IDataSet<T>> pr) {
            HillviewLogging.logger().info("Received partial result from ", this.name);
            if (this.session == null)
                return;
            if (!this.session.isOpen()) {
                HillviewLogging.logger().warn("Session closed, ignoring partial result");
                return;
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            IDataSet<T> dataSet = pr.deltaValue;
            // Replace the "data" with the remote object ID
            if (dataSet != null) {
                this.result = dataSet;
                RpcTarget target = this.factory.apply(this.result, null);
                json.addProperty("data", target.objectId);
            } else {
                json.add("data", null);
            }
            RpcReply reply = this.request.createReply(json);
            reply.send(this.session);
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
     * @param session Web socket session.
     */
    <T, R extends IJson> void
    runSketch(IDataSet<T> data, ISketch<T, R> sketch,
              RpcRequest request, Session session) {
        // Run the sketch
        Observable<PartialResult<R>> sketches = data.sketch(sketch);
        // Knows how to add partial results
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        // Prefix sum of the partial results
        Observable<PartialResult<R>> add = sketches.scan(prm::add);
        // Send the partial results back
        SketchResultObserver<R> robs = new SketchResultObserver<R>(
                sketch.toString(), this, request, session);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }

    /**
     * Runs a sketch and sends the complete sketch result received directly to the client.
     * Progress updates are sent to the client, but accompanied by null values.
     * @param data    Dataset to run the sketch on.
     * @param sketch  Sketch to run.
     * @param postprocessing  This function is applied to the sketch results.
     * @param request Web socket request, where replies are sent.
     * @param session Web socket session.
     */
    <T, R, S extends IJson> void
    runCompleteSketch(IDataSet<T> data, ISketch<T, R> sketch,
                      BiFunction<R, HillviewComputation, S> postprocessing,
                      RpcRequest request, Session session) {
        // Run the sketch
        Observable<PartialResult<R>> sketches = data.sketch(sketch);
        // Knows how to add partial results
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        // Prefix sum of the partial results
        Observable<PartialResult<R>> add = sketches.scan(prm::add);
        // Send the partial results back
        CompleteSketchResultObserver<T, R, S> robs = new CompleteSketchResultObserver<T, R, S>(
                sketch.toString(), this, request, session, postprocessing);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }

    /**
     * Runs a map and sends the result directly to the client.
     * @param data    Dataset to run the map on.
     * @param map     Map to execute.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param session Web socket session.
     */
    <T, S> void
    runMap(IDataSet<T> data, IMap<T, S> map,
           BiFunction<IDataSet<S>, HillviewComputation, RpcTarget> factory,
           RpcRequest request, Session session) {
        // Run the map
        Observable<PartialResult<IDataSet<S>>> stream = data.map(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                map.toString(), this, request, session, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }

    /**
     * Runs a flatmap and sends the result directly to the client.
     * @param data    Dataset to run the map on.
     * @param map     Map to execute.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param session Web socket session.
     */
    <T, S> void
    runFlatMap(IDataSet<T> data, IMap<T, List<S>> map,
               BiFunction<IDataSet<S>, HillviewComputation, RpcTarget> factory,
               RpcRequest request, Session session) {
        // Run the flatMap
        Observable<PartialResult<IDataSet<S>>> stream = data.flatMap(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                map.toString(), this, request, session, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }

    /**
     * Runs a zip between two datasets.
     * @param data    Left dataset.
     * @param other   Right dataset.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param session Web socket session.
     */
    <T, S> void
    runZip(IDataSet<T> data, IDataSet<S> other,
           BiFunction<IDataSet<Pair<T, S>>, HillviewComputation, RpcTarget> factory,
           RpcRequest request, Session session) {
        Observable<PartialResult<IDataSet<Pair<T, S>>>> stream = data.zip(other);
        PRDataSetMonoid<Pair<T, S>> monoid = new PRDataSetMonoid<Pair<T, S>>();
        Observable<PartialResult<IDataSet<Pair<T, S>>>> add = stream.scan(monoid::add);
        // We can actually reuse the MapResultObserver
        MapResultObserver<Pair<T, S>> robs = new MapResultObserver<Pair<T, S>>(
                                "zip", this, request, session, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }

    /**
     * Runs a management command and sends the data received directly to the client.
     * @param data    Dataset to run the manage command on.
     * @param command Command to run.
     * @param request Web socket request, where replies are sent.
     * @param session Web socket session.
     */
    <T, R extends IJson> void
    runManage(IDataSet<T> data, ControlMessage command,
              RpcRequest request, Session session) {
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
                    command.toString(), this, request, session);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }
}

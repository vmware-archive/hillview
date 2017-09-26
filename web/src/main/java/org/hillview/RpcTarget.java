/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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
import org.hillview.utils.Converters;
import rx.Observable;
import rx.Observer;
import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class RpcTarget implements IJson {
    @Nullable // This is null for a very brief time
    String objectId;
    private final HashMap<String, Method> executor;
    static final Logger logger = Logger.getLogger(RpcTarget.class.getName());

    // This constructor is only called by the InitialObjectTarget, who
    // must have a fixed object Id.
    protected RpcTarget(String id) {
        this.executor = new HashMap<String, Method>();
        this.registerExecutors();
        this.objectId = id;
        RpcObjectManager.instance.addObject(this);
    }

    RpcTarget() {
        this.executor = new HashMap<String, Method>();
        this.registerExecutors();
        RpcObjectManager.instance.addObject(this);
    }

    public void setId(String objectId) {
        this.objectId = objectId;
    }

    private synchronized void saveSubscription(Session session, Subscription sub) {
        RpcObjectManager.instance.addSubscription(session, sub);
    }

    /**
     * Use reflection to register all methods that have an @HillviewRpc annotation.
     * These methods will be invoked for each RpcRequest received.
     * All these methods should have the following signature:
     * method(RpcRequest req, Session session).
     * The method is responsible for:
     * - parsing the arguments of the RpcCall
     * - sending the replies, in any number they may be, using the session
     * - closing the session on termination.
     */
    private void registerExecutors() {
        Class<?> type = this.getClass();
        for (Method m : type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(HillviewRpc.class)) {
                logger.log(Level.INFO, "Registered RPC method " + m.getName());
                this.executor.put(m.getName(), m);
            }
        }
    }

    /**
     * Dispatches an RPC request for execution.
     * This will look up the method in the RpcRequest using reflection
     * and invoke it using Java reflection.
     */
    void execute(RpcRequest request, Session session)
            throws InvocationTargetException, IllegalAccessException {
        Method cons = this.executor.get(request.method);
        if (cons == null)
            throw new RuntimeException(this.toString() + ": No such method " + request.method);
        cons.invoke(this, request, session);
    }

    @Override
    public int hashCode() {
        return Converters.checkNull(this.objectId).hashCode();
    }

    abstract class ResultObserver<T> implements Observer<PartialResult<T>> {
        final RpcRequest request;
        final Session session;
        final String name;

        ResultObserver(String name, RpcRequest request, Session session) {
            this.name = name;
            this.request = request;
            this.session = session;
        }

        @Override
        public void onCompleted() {
            logger.log(Level.INFO, "Computation completed for " + this.name);
            this.request.syncCloseSession(this.session);
            RpcObjectManager.instance.removeSubscription(this.session);
        }

        @Override
        public void onError(Throwable throwable) {
            if (!this.session.isOpen()) return;

            RpcTarget.logger.log(Level.SEVERE, this.name + " onError");
            RpcTarget.logger.log(Level.SEVERE, throwable.toString());
            RpcReply reply = this.request.createReply(throwable);
            reply.send(this.session);
        }
    }

    class SketchResultObserver<T extends IJson> extends ResultObserver<T> {
        SketchResultObserver(String name, RpcRequest request, Session session) {
            super(name, request, session);
        }

        @Override
        public void onNext(PartialResult<T> pr) {
            logger.log(Level.INFO, "Received partial result from " + this.name);
            if (!this.session.isOpen()) {
                logger.log(Level.WARNING, "Session closed, ignoring partial result");
                return;
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            if (pr.deltaValue == null)
                json.add("data", null);
            else
                json.add("data", pr.deltaValue.toJsonTree());
            RpcReply reply = this.request.createReply(json);
            reply.send(this.session);
        }
    }

    class MapResultObserver<T> extends ResultObserver<IDataSet<T>> {
        @Nullable
        IDataSet<T> result;
        final Function<IDataSet<T>, RpcTarget> factory;

        MapResultObserver(String name, RpcRequest request,
                          Session session, Function<IDataSet<T>, RpcTarget> factory) {
            super(name, request, session);
            this.factory = factory;
        }

        @Override
        public void onNext(PartialResult<IDataSet<T>> pr) {
            logger.log(Level.INFO, "Received partial result from " + this.name);
            if (!this.session.isOpen()) {
                logger.log(Level.WARNING, "Session closed, ignoring partial result");
                return;
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            IDataSet<T> dataSet = pr.deltaValue;
            // Replace the "data" with the remote object ID
            if (dataSet != null) {
                this.result = dataSet;
                RpcTarget target = this.factory.apply(this.result);
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
                sketch.toString(), request, session);
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
    runCompleteSketch(IDataSet<T> data, ISketch<T, R> sketch, Function<R, S> postprocessing,
              RpcRequest request, Session session) {
        // Run the sketch
        Observable<PartialResult<R>> sketches = data.sketch(sketch);
        // Knows how to add partial results
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch);
        // Prefix sum of the partial results.
        // publish().autoConnect(2) ensures that the two consumers
        // of this stream pull from the *same* stream, and not from
        // two different copies; the two consumers are lastSketch and progress.
        Observable<PartialResult<R>> add = sketches.scan(prm::add).publish().autoConnect(2);
        Observable<PartialResult<S>> lastSketch = add.last()
                .map(p -> new PartialResult<S>(p.deltaDone, postprocessing.apply(p.deltaValue)));
        Observable<PartialResult<S>> progress = add.map(p -> new PartialResult<S>(p.deltaDone, null));
        Observable<PartialResult<S>> result = progress.mergeWith(lastSketch);
        SketchResultObserver<S> robs = new SketchResultObserver<S>(
                sketch.toString(), request, session);
        Subscription sub = result.subscribe(robs);
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
    runMap(IDataSet<T> data, IMap<T, S> map, Function<IDataSet<S>, RpcTarget> factory,
              RpcRequest request, Session session) {
        // Run the map
        Observable<PartialResult<IDataSet<S>>> stream = data.map(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                map.toString(), request, session, factory);
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
    runFlatMap(IDataSet<T> data, IMap<T, List<S>> map, Function<IDataSet<S>, RpcTarget> factory,
               RpcRequest request, Session session) {
        // Run the flatMap
        Observable<PartialResult<IDataSet<S>>> stream = data.flatMap(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                map.toString(), request, session, factory);
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
           Function<IDataSet<Pair<T, S>>, RpcTarget> factory,
           RpcRequest request, Session session) {
        Observable<PartialResult<IDataSet<Pair<T, S>>>> stream = data.zip(other);
        PRDataSetMonoid<Pair<T, S>> monoid = new PRDataSetMonoid<Pair<T, S>>();
        Observable<PartialResult<IDataSet<Pair<T, S>>>> add = stream.scan(monoid::add);
        // We can actually reuse the MapResultObserver
        MapResultObserver<Pair<T, S>> robs = new MapResultObserver<Pair<T, S>>(
                                "zip", request, session, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(session, sub);
    }
}

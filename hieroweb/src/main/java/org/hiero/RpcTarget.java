/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hiero;

import com.google.gson.JsonObject;
import org.hiero.sketch.dataset.api.*;
import org.hiero.sketch.dataset.*;
import org.hiero.utils.Converters;
import rx.Observable;
import rx.Observer;
import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.hiero.utils.Converters.checkNull;

public abstract class RpcTarget {
    @Nullable // This is null for a very brief time
    String objectId;
    private final HashMap<String, Method> executor;
    protected static final Logger logger = Logger.getLogger(RpcTarget.class.getName());

    @Nullable
    protected Subscription subscription;

    RpcTarget() {
        this.executor = new HashMap<String, Method>();
        this.registerExecutors();
        RpcObjectManager.instance.addObject(this);
        this.subscription = null;
    }

    public void setId(String objectId) {
        this.objectId = objectId;
    }

    synchronized void cancel() {
        logger.log(Level.INFO, "Cancelling " + this.toString());
        if (this.subscription != null) {
            logger.log(Level.INFO, "Unsubscribing " + this.toString());
            this.subscription.unsubscribe();
            this.subscription = null;
        }
    }

    private synchronized void saveSubscription(Subscription sub) {
        if (sub.isUnsubscribed())
            // The computation may have already finished by the time we get here!
            return;
        logger.log(Level.INFO, "Saving subscription " + this.toString());
        if (this.subscription != null)
            throw new RuntimeException("Subscription already active");
        this.subscription = sub;
    }

    private synchronized void removeSubscription() {
        if (this.subscription == null)
            return;
        logger.log(Level.INFO, "Removing subscription " + this.toString());
        this.subscription = null;
    }

    /**
     * Use reflection to register all methods that have an @HieroRpc annotation.
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
            if (m.isAnnotationPresent(HieroRpc.class)) {
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

        ResultObserver(RpcRequest request, Session session) {
            this.request = request;
            this.session = session;
        }

        @Override
        public void onCompleted() {
            this.request.syncCloseSession(this.session);
            RpcTarget.this.removeSubscription();
        }

        @Override
        public void onError(Throwable throwable) {
            if (!this.session.isOpen()) return;

            RpcTarget.logger.log(Level.SEVERE, throwable.toString());
            RpcReply reply = this.request.createReply(throwable);
            reply.send(this.session);
        }
    }

    class SketchResultObserver<T extends IJson> extends ResultObserver<T> {
        SketchResultObserver(RpcRequest request, Session session) {
            super(request, session);
        }

        @Override
        public void onNext(PartialResult<T> pr) {
            logger.log(Level.INFO, "Received partial result");
            if (!this.session.isOpen()) {
                logger.log(Level.WARNING, "Session closed, ignoring partial result");
                return;
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            T delta = checkNull(pr.deltaValue);
            json.add("data", delta.toJsonTree());
            RpcReply reply = this.request.createReply(json);
            reply.send(this.session);
        }
    }

    class MapResultObserver<T> extends ResultObserver<IDataSet<T>> {
        @Nullable
        IDataSet<T> result;
        final Function<IDataSet<T>, RpcTarget> factory;

        MapResultObserver(RpcRequest request, Session session, Function<IDataSet<T>, RpcTarget> factory) {
            super(request, session);
            this.factory = factory;
        }

        @Override
        public void onNext(PartialResult<IDataSet<T>> pr) {
            logger.log(Level.INFO, "Received partial result");
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

    String idToJson() {
        return IJson.gsonInstance.toJson(this.objectId);
    }

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
        SketchResultObserver<R> robs = new SketchResultObserver<R>(request, session);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(sub);
    }

    <T, S> void
    runMap(IDataSet<T> data, IMap<T, S> map, Function<IDataSet<S>, RpcTarget> factory,
              RpcRequest request, Session session) {
        // Run the sketch
        Observable<PartialResult<IDataSet<S>>> stream = data.map(map);
        // Knows how to add partial results
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(request, session, factory);
        Subscription sub = add.subscribe(robs);
        this.saveSubscription(sub);
    }
}

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
import io.grpc.StatusRuntimeException;
import org.hillview.dataset.api.*;
import org.hillview.dataset.*;
import org.hillview.utils.*;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.exceptions.CompositeException;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An RPC target is an object that has methods that are invoked from the UI
 * through the web server.  All these methods are tagged with @HillviewRpc.
 * When these objects are serialized as JSON only their String id is sent; the
 * objects always reside on the web server.  They are managed by the RpcObjectManager.
 * Note: all classes subclassing RpcTarget that have @HillviewRpc methods
 * must be public for reflection to work properly.
 */
public abstract class RpcTarget implements IJson, IRpcTarget {
    static final long serialVersionUID = 1;

    private final IRpcTarget.Id objectId;
    /**
     * Computation that has generated this object.  Can only
     * be null for the initial object.
     */
    @Nullable
    public final HillviewComputation computation;

    @Override
    public IRpcTarget.Id getId() {
        return this.objectId;
    }

    /**
     * This constructor is only called for the InitialObject.
     */
    protected RpcTarget() {
        this.objectId = RpcObjectManager.initialObjectId;
        this.computation = null;
    }

    protected RpcTarget(HillviewComputation computation) {
        this.computation = computation;
        this.objectId = computation.resultId;
        HillviewLogger.instance.info("Create RpcTarget", "{0}", computation.toString());
    }

    /**
     * Insert object in object manager maps.
     * Also, notify computations that may be waiting for object to appear.
     * This method should be called last thing after the construction of the
     * object has been completed.
     */
    protected void registerObject() {
        RpcObjectManager.instance.addObject(this);
        if (this.computation != null)
            this.computation.objectCreated(this);
    }

    private synchronized void saveSubscription(RpcRequestContext context, Subscription sub) {
        RpcObjectManager.instance.addSubscription(context, sub);
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
     * The method will most often end by calling runSketch, runMap, etc --
     * one of the methods below.
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
                throw new HillviewException(this.toString() + ": No such method " + request.method);
            HillviewLogger.instance.info("Executing", "request={0}, context={1}",
                    request.toString(), context.toString());
            method.invoke(this, request, context);
        } catch (Exception ex) {
            HillviewLogger.instance.error("Exception while invoking method", ex);
            RpcReply reply = request.createReply(ex);
            if (context.session != null)
                RpcServer.sendReply(reply, context.session);
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

    abstract static class ResultObserver<T> implements Observer<PartialResult<T>> {
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

        void sendReply(final RpcReply reply) {
            RpcServer.sendReply(reply, Converters.checkNull(this.context.session));
        }

        @Override
        public void onCompleted() {
            HillviewLogger.instance.info("Computation completed", "for {0}", this.name);
            RpcServer.requestCompleted(this.request, Converters.checkNull(this.context.session));
            this.request.syncCloseSession(this.context.session);
        }

        @Override
        public void onError(Throwable throwable) {
            HillviewLogger.instance.error("onError", "{0}", this.name);
            HillviewLogger.instance.error("onError", throwable);
            boolean reconstructing = this.checkMissingDataset(throwable);
            if (reconstructing ||
                    this.context.session == null ||
                    !this.context.session.isOpen()) return;
            HillviewLogger.instance.info("Sending exception to user", "{0}", this.name);
            RpcReply reply = this.request.createReply(throwable);
            this.sendReply(reply);
        }

        HillviewComputation getComputation() {
            return this.context.getComputation(this.request);
        }

        /**
         * Checks whether an exception indicates that a dataset has been
         * removed by a worker, and it may need to be reconstructed.
         * Returns true if the reconstruction is attempted.
         */
        boolean checkMissingDataset(Throwable throwable) {
            List<Throwable> exceptions;
            if (throwable instanceof CompositeException) {
                CompositeException ce = (CompositeException)throwable;
                exceptions = ce.getExceptions();
            } else {
                exceptions = new ArrayList<Throwable>();
                exceptions.add(throwable);
            }

            boolean datasetMissing = false;
            for (Throwable t: exceptions) {
                if (!(throwable instanceof StatusRuntimeException))
                    continue;
                StatusRuntimeException sre = (StatusRuntimeException)t;
                String description = sre.getStatus().getDescription();
                if (description != null && description.contains("DatasetMissing")) {
                    datasetMissing = true;
                    break;
                }
            }

            if (datasetMissing) {
                RpcTarget.Id[] toDelete = this.request.getDatasetSourceIds();
                for (RpcTarget.Id s: toDelete) {
                    HillviewLogger.instance.info("Trying to rebuild missing remote object", "{0}", s);
                    RpcObjectManager.instance.deleteObject(s);
                }
                // Try to re-execute this request; this will trigger rebuilding the sources.
                RpcServer.execute(this.request, this.context);
                return true;
            }

            HillviewLogger.instance.info("Dataset is not missing");
            return false;
        }
    }

    /**
     * Observes a sketch computation and applies a postprocessing function to intermediate results
     * before returning them.
     * @param <R> Type of data.
     */
    static class SketchResultObserver<R, S extends IJson> extends ResultObserver<R> {
        private final Function<R, S> postprocessing;

        SketchResultObserver(String name, RpcTarget target, RpcRequest request,
                             RpcRequestContext context,
                             Function<R, S> postprocessing) {
            super(name, request, target, context);
            this.postprocessing = postprocessing;
        }

        @Override
        public void onNext(PartialResult<R> pr) {
            HillviewLogger.instance.info("Received partial sketch", "from {0}", this.name);
            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;

            try {
                @Nullable
                S result = this.postprocessing.apply(pr.deltaValue);
                JsonObject json = new JsonObject();
                json.addProperty("done", pr.deltaDone);
                if (result == null)
                    json.add("data", null);
                else
                    json.add("data", result.toJsonTree());
                RpcReply reply = this.request.createReply(json);
                this.sendReply(reply);
            } catch (Exception ex) {
                HillviewLogger.instance.error("Exception during serialization to JSON", ex);
                super.onError(ex);
            }
        }
    }

    /**
     * This observes a sketch computation, but only sends the final sketch result
     * to the consumer.
     * @param <R> Type of data from sketch.
     * @param <S> Type of data sent to client.
     */
    static class CompleteSketchResultObserver<R, S extends IJson> extends ResultObserver<R> {
        /**
         * True when a result was received and send to client.
         */
        private boolean resultReceived;
        @Nullable
        private R last;
        private final Function<R, S> postprocessing;

        CompleteSketchResultObserver(String name, RpcTarget target, RpcRequest request,
                                     RpcRequestContext context,
                                     Function<R, S> postprocessing) {
            super(name, request, target, context);
            this.last = null;
            this.postprocessing = postprocessing;
            this.resultReceived = false;
        }

        @Override
        public void onNext(PartialResult<R> pr) {
            HillviewLogger.instance.info("Received partial sketch", "from {0}", this.name);
            this.last = pr.deltaValue;
            this.resultReceived = true;
            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            // always send null data for partial results
            json.add("data", null);
            RpcReply reply = this.request.createReply(json);
            this.sendReply(reply);
        }

        public void onError(Throwable t) {
            super.onError(t);
            this.resultReceived = true;
        }

        @Override
        public void onCompleted() {
            if (!this.resultReceived) {
                HillviewLogger.instance.error(
                        "Completing message without result", "for {0}:{1}",
                        this.name, Utilities.stackTraceString());
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", 1.0);
            try {
                @Nullable S result = this.postprocessing.apply(this.last);
                HillviewLogger.instance.info("Computation completed", "for {0}", this.name);
                Session session = this.context.getSessionIfOpen();
                if (session == null)
                    return;

                if (result == null)
                    json.add("data", null);
                else
                    json.add("data", result.toJsonTree());
            } catch (Exception e) {
                HillviewLogger.instance.error("Exception during onCompleted", e);
                super.onError(e);
                return; // no not send any other replies
            }

            RpcReply reply = this.request.createReply(json);
            this.sendReply(reply);
            super.onCompleted();
        }
    }

    static class MapResultObserver<T> extends ResultObserver<IDataSet<T>> {
        @Nullable
        IDataSet<T> result;
        final BiFunction<IDataSet<T>, HillviewComputation, IRpcTarget> factory;

        MapResultObserver(String name, RpcTarget target, RpcRequest request,
                          RpcRequestContext context,
                          BiFunction<IDataSet<T>, HillviewComputation, IRpcTarget> factory) {
            super(name, request, target, context);
            this.factory = factory;
        }

        @Override
        public void onNext(PartialResult<IDataSet<T>> pr) {
            HillviewLogger.instance.info("Received partial map", "from {0}", this.name);

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            IDataSet<T> dataSet = pr.deltaValue;
            // Replace the "data" with the remote object ID
            if (dataSet != null) {
                this.result = dataSet;
                IRpcTarget target = this.factory.apply(this.result, this.getComputation());
                json.addProperty("data", target.getId().toString());
            } else {
                json.add("data", null);
            }

            Session session = this.context.getSessionIfOpen();
            if (session == null)
                return;
            RpcReply reply = this.request.createReply(json);
            this.sendReply(reply);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "::" + this.objectId;
    }

    /**
     * Default serialization of an RpcTarget is the object id.
     */
    @Override
    public JsonElement toJsonTree() {
        return IJson.gsonInstance.toJsonTree(this.objectId.toString());
    }

    private <T, R extends ISketchResult, S extends IJson> void
    runObservedSketch(IDataSet<T> data, PostProcessedSketch<T, R, S> sketch, ResultObserver<R> observer,
              RpcRequestContext context) {
        // Run the sketch
        Observable<PartialResult<R>> sketches = data.sketch(sketch.sketch);
        // Knows how to add partial results
        PartialResultMonoid<R> prm = new PartialResultMonoid<R>(sketch.sketch);
        // Prefix sum of the partial results
        Observable<PartialResult<R>> add = sketches.scan(prm::add);
        Subscription sub = add
                .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .subscribe(observer);
        this.saveSubscription(context, sub);
    }

    @Override
    public <T, R extends ISketchResult, S extends IJson> void
    runSketch(IDataSet<T> data, PostProcessedSketch<T, R, S> sketch,
              RpcRequest request, RpcRequestContext context) {
        SketchResultObserver<R, S> robs =
                new SketchResultObserver<R, S>(
                        sketch.sketch.asString(), this, request, context, sketch::postProcess);
        this.runObservedSketch(data, sketch, robs, context);
    }

    @Override
    public <T, R extends ISketchResult, S extends IJson> void
    runCompleteSketch(IDataSet<T> data, PostProcessedSketch<T, R, S> sketch,
                      RpcRequest request, RpcRequestContext context) {
        CompleteSketchResultObserver<R, S> robs = new CompleteSketchResultObserver<R, S>(
                sketch.sketch.asString(), this, request, context, sketch::postProcess);
        this.runObservedSketch(data, sketch, robs, context);
    }

    /**
     * Helper function which runs a streaming computation that returns a dataset.
     * @param stream       Result produced by computation.
     * @param description  Description of the computation.
     * @param request      Web request.
     * @param context      Web request context.
     * @param factory      Knows how to allocate a Target to hold the resulting dataset.
     * @param <S>          Type of data in result stream.
     */
    private <S> void collectDataset(Observable<PartialResult<IDataSet<S>>> stream,
                                    String description,
                                    RpcRequest request, RpcRequestContext context,
                                    BiFunction<IDataSet<S>, HillviewComputation, IRpcTarget> factory) {
        PRDataSetMonoid<S> monoid = new PRDataSetMonoid<S>();
        // Prefix sum of the partial results
        Observable<PartialResult<IDataSet<S>>> add = stream.scan(monoid::add);
        // Send the partial results back
        MapResultObserver<S> robs = new MapResultObserver<S>(
                description, this, request, context, factory);
        Subscription sub = add
                .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .subscribe(robs);
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
    @Override
    public <T, S> void
    runMap(IDataSet<T> data, IMap<T, S> map,
           BiFunction<IDataSet<S>, HillviewComputation, IRpcTarget> factory,
           RpcRequest request, RpcRequestContext context) {
        Observable<PartialResult<IDataSet<S>>> stream = data.map(map);
        this.collectDataset(stream, map.asString(), request, context, factory);
    }

    /**
     * Runs a prune and sends the result directly to the client.
     * @param data    Dataset to run the map on.
     * @param map     Map to execute.
     * @param factory Function which knows how to create a new RpcTarget
     *                out of the resulting IDataSet.  It is the reference
     *                to this RpcTarget that is returned to the client.
     * @param request Web socket request, used to send the reply.
     * @param context Context for the computation.
     */
    @Override
    public <T> void
    runPrune(IDataSet<T> data, IMap<T, Boolean> map,
             BiFunction<IDataSet<T>, HillviewComputation, IRpcTarget> factory,
             RpcRequest request, RpcRequestContext context) {
        Observable<PartialResult<IDataSet<T>>> stream = data.prune(map);
        this.collectDataset(stream, map.asString(), request, context, factory);
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
    @Override
    public <T, S> void
    runFlatMap(IDataSet<T> data, IMap<T, List<S>> map,
               BiFunction<IDataSet<S>, HillviewComputation, IRpcTarget> factory,
               RpcRequest request, RpcRequestContext context) {
        Observable<PartialResult<IDataSet<S>>> stream = data.flatMap(map);
        this.collectDataset(stream, map.asString(), request, context, factory);
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
    @Override
    public <T, S> void
    runZip(IDataSet<T> data, IDataSet<S> other,
           BiFunction<IDataSet<Pair<T, S>>, HillviewComputation, IRpcTarget> factory,
           RpcRequest request, RpcRequestContext context) {
        Observable<PartialResult<IDataSet<Pair<T, S>>>> stream = data.zip(other);
        this.collectDataset(stream, "zip", request, context, factory);
    }

    /**
     * Runs a management command and sends the data received directly to the client.
     * @param data    Dataset to run the manage command on.
     * @param command Command to run.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    @Override
    public <T> void
    runManage(IDataSet<T> data, ControlMessage command,
              RpcRequest request, RpcRequestContext context) {
        // Run the sketch
        Observable<PartialResult<ControlMessage.StatusList>> sketches = data.manage(command);
        // Knows how to add partial results
        PartialResultMonoid<ControlMessage.StatusList> prm =
                new PartialResultMonoid<ControlMessage.StatusList>(
                        new ControlMessage.StatusListMonoid());
        // Prefix sum of the partial results
        Observable<PartialResult<ControlMessage.StatusList>> add = sketches.scan(prm::add);
        // Send the partial results back
        SketchResultObserver<ControlMessage.StatusList, ControlMessage.StatusList> robs =
                new SketchResultObserver<ControlMessage.StatusList, ControlMessage.StatusList>(
                        command.toString(), this, request, context, e -> e);
        Subscription sub = add
                .unsubscribeOn(ExecutorUtils.getUnsubscribeScheduler())
                .subscribe(robs);
        this.saveSubscription(context, sub);
    }
}

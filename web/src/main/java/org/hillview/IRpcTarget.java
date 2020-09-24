/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import org.hillview.sketches.highorder.IdPostProcessedSketch;
import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.utils.ICast;
import org.hillview.utils.Pair;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

public interface IRpcTarget extends ICast {
    /**
     * This class represents the ID of an RPC Target.
     * It is used by other classes that refer to RpcTargets by their ids.
     */
    class Id {
        private final String objectId;
        public Id(String objectId) {
            this.objectId = objectId;
        }

        /**
         * Allocate a fresh identifier.
         */
        static Id freshId() {
            return new RpcTarget.Id(UUID.randomUUID().toString());
        }

        static Id initialId() {
            return new RpcTarget.Id(Integer.toString(RemoteDataSet.defaultDatasetIndex));
        }

        @Override
        public String toString() {
            return this.objectId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;

            Id id = (Id) o;
            return this.objectId.equals(id.objectId);
        }

        @Override
        public int hashCode() {
            return this.objectId.hashCode();
        }

        boolean isInitial() {
            return this.objectId.equals(Id.initialId().objectId);
        }
    }

    Id getId();

    /**
     * Runs a sketch with post processing
     * and sends the data received directly to the client.
     * @param data    Dataset to run the sketch on.
     * @param sketch  Post-processed sketch to run.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    <T, R extends ISketchResult, S extends IJson> void
    runSketch(IDataSet<T> data, PostProcessedSketch<T, R, S> sketch,
              RpcRequest request, RpcRequestContext context);

    /**
     * Return this result directly to the caller.
     * @param result  Result to return.
     * @param request Web socket request.
     * @param context Context for the computation
     * @param <R>     Type of the result.
     */
    <R extends IJson> void
    returnResult(@Nullable R result, RpcRequest request, RpcRequestContext context);

    /**
     * Runs a sketch and sends the data received directly to the client.
     * @param data    Dataset to run the sketch on.
     * @param sketch  Sketch to run.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    default <T, R extends IJsonSketchResult> void
    runSketch(IDataSet<T> data, ISketch<T, R> sketch,
              RpcRequest request, RpcRequestContext context) {
        IdPostProcessedSketch<T, R> id = new IdPostProcessedSketch<T, R>(sketch);
        this.runSketch(data, id, request, context);
    }

    /**
     * Runs a sketch and sends the complete sketch result received directly to the client.
     * Progress updates are sent to the client, but accompanied by null values.
     * @param data    Dataset to run the sketch on.
     * @param sketch  Sketch to run.
     * @param request Web socket request, where replies are sent.
     * @param context Context for the computation.
     */
    <T, R extends ISketchResult, S extends IJson> void
    runCompleteSketch(IDataSet<T> data, PostProcessedSketch<T, R, S> sketch,
                      RpcRequest request, RpcRequestContext context);

    default <T, R extends IJsonSketchResult> void
    runCompleteSketch(IDataSet<T> data, ISketch<T, R> sketch,
                      RpcRequest request, RpcRequestContext context) {
        IdPostProcessedSketch<T, R> post = new IdPostProcessedSketch<T, R>(sketch);
        this.runCompleteSketch(data, post, request, context);
    }

    <T, S> void
    runMap(IDataSet<T> data, IMap<T, S> map,
           BiFunction<IDataSet<S>, HillviewComputation, IRpcTarget> factory,
           RpcRequest request, RpcRequestContext context);

    <T> void
    runPrune(IDataSet<T> data, IMap<T, Boolean> map,
             BiFunction<IDataSet<T>, HillviewComputation, IRpcTarget> factory,
             RpcRequest request, RpcRequestContext context);

    <T, S> void
    runFlatMap(IDataSet<T> data, IMap<T, List<S>> map,
               BiFunction<IDataSet<S>, HillviewComputation, IRpcTarget> factory,
               RpcRequest request, RpcRequestContext context);

    <T, S, R> void
    runZip(IDataSet<T> data, IDataSet<S> other, IMap<Pair<T, S>, R> map,
           BiFunction<IDataSet<R>, HillviewComputation, IRpcTarget> factory,
           RpcRequest request, RpcRequestContext context);

    <T, R> void
    runZipN(IDataSet<T> data, List<IDataSet<T>> other, IMap<List<T>, R> map,
           BiFunction<IDataSet<R>, HillviewComputation, IRpcTarget> factory,
           RpcRequest request, RpcRequestContext context);

    <T> void
    runManage(IDataSet<T> data, ControlMessage command,
              RpcRequest request, RpcRequestContext context);
}

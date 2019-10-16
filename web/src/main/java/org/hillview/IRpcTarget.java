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

import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;

import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;

public interface IRpcTarget {
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

    <T, R, S extends IJson> void
    runSketchPostprocessing(IDataSet<T> data, ISketch<T, R> sketch,
                            BiFunction<R, HillviewComputation, S> postprocessing,
                            RpcRequest request, RpcRequestContext context);

    <T, R extends IJson> void
    runSketch(IDataSet<T> data, ISketch<T, R> sketch,
              RpcRequest request, RpcRequestContext context);

    <T, R, S extends IJson> void
    runCompleteSketch(IDataSet<T> data, ISketch<T, R> sketch,
                      BiFunction<R, HillviewComputation, S> postprocessing,
                      RpcRequest request, RpcRequestContext context);

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

    <T, S> void
    runZip(IDataSet<T> data, IDataSet<S> other,
           BiFunction<IDataSet<Pair<T, S>>, HillviewComputation, IRpcTarget> factory,
           RpcRequest request, RpcRequestContext context);

    <T> void
    runManage(IDataSet<T> data, ControlMessage command,
              RpcRequest request, RpcRequestContext context);
}

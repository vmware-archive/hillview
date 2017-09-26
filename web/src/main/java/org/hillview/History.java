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

import org.hillview.dataset.api.IDataSetComputation;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class keeps track of the history of computations and their results.
 * It allows one to reconstruct results which no longer exist.
 */
public class History {
    enum ComputationKind {
        Map,
        FlatMap,
        Sketch,
        Zip
    }

    /**
     * Describes a computation performed on a RpcTarget.
     */
    static class Computation {
        /**
         * Kind of computation that was executed.
         */
        final ComputationKind     kind;
        /**
         * The actual computation that was executed.
         * For zip this is null.
         */
        @Nullable
        final IDataSetComputation computation;
        /**
         * Unique identifier of input RpcTarget dataset.
         */
        final String           source;
        /**
         * For zip this is the unique identifier of the second RpcTarget input.
         * Otherwise it's null.
         */
        @Nullable
        final String           secondSource;

        Computation(ComputationKind kind, @Nullable IDataSetComputation computation,
                           String source, @Nullable String secondSource) {
            this.kind = kind;
            this.computation = computation;
            this.source = source;
            this.secondSource = secondSource;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;

            Computation that = (Computation) o;
            return this.kind == that.kind &&
                    (this.computation != null ? this.computation.equals(that.computation) : that.computation == null) &&
                    this.source.equals(that.source) &&
                    (this.secondSource != null ? this.secondSource.equals(that.secondSource) : that.secondSource == null);
        }

        @Override
        public int hashCode() {
            int result = this.kind.hashCode();
            result = 31 * result + (this.computation != null ? this.computation.hashCode() : 0);
            result = 31 * result + this.source.hashCode();
            result = 31 * result + (this.secondSource != null ? this.secondSource.hashCode() : 0);
            return result;
        }
    }

    /**
     * Maps each generated result to the computation that has produced it.
     */
    private final Map<String, Computation> generator;
    /**
     * Maps each computation to the result it produced.
     * For maps and zips the result is the RpcTarget object id.
     * For sketches the result is the actual sketch result.
     */
    private final Map<Computation, Object> computationResult;

    public History() {
        this.generator = new HashMap<String, Computation>();
        this.computationResult = new HashMap<Computation, Object>();
    }

    public void add(RpcTarget source, @Nullable RpcTarget secondSource,
                    ComputationKind kind, @Nullable IDataSetComputation comp,
                    RpcTarget result) {
        Computation c = new Computation(kind, comp, Converters.checkNull(source.objectId),
                secondSource != null ? secondSource.objectId : null);
        this.generator.put(result.objectId, c);
    }

    public Computation get(String resultId) {
        return this.generator.get(resultId);
    }
}

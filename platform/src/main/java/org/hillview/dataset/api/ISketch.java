/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

package org.hillview.dataset.api;

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Describes a sketch computation on a dataset of type T that produces a result of type R.
 * This class is also a monoid which knows how to combine two values of type R using the add
 * method.  Sketch objects have to be immutable once created.
 * @param <T> Input data type.
 * @param <R> Output data type.
 */
public interface ISketch<T, R extends ISketchResult> extends
        IDataSetComputation, IMonoid<R> {
    /**
     * Sketch computation on some dataset T.
     * @param data  Data to sketch.
     * @return  A sketch of the data.
     */
    @Nullable
    R create(@Nullable T data);

    /**
     * Helper method to return non-null zeros.
     */
    @Nullable
    default R getZero() { return Converters.checkNull(this.zero()); }

    /**
     * Creates a post-processed sketch which runs the specified post-processing
     * function after the sketch completes.
     * @param post  Post processing function to execute.
     * @param <F>   Final type of result produced.
     */
    default <F extends IJson> PostProcessedSketch<T, R, F> andThen(Function<R, F> post) {
        return new PostProcessedSketch<T, R, F>(this) {
            @Nullable
            @Override
            public F postProcess(@Nullable R result) {
                return post.apply(result);
            }
        };
    }
}

/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches.highorder;

import org.hillview.dataset.api.*;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * A sketch bundled with a post-processing function.
 * @param <T>  Sketch input data.
 * @param <R>  Sketch output data.
 * @param <F>  Data produced after postprocessing.
 */
public abstract class PostProcessedSketch<T, R extends ISketchResult, F extends IJson> {
    public final ISketch<T, R> sketch;

    protected PostProcessedSketch(ISketch<T, R> sketch) {
        this.sketch = sketch;
    }

    @Nullable
    public abstract F postProcess(@Nullable R result);

    public <F1 extends IJson> PostProcessedSketch<T, R, F1> andThen(Function<F, F1> after) {
        return new PostProcessedSketch<T, R, F1>(this.sketch) {
            @Nullable
            @Override
            public F1 postProcess(@Nullable R result) {
                return after.apply(PostProcessedSketch.this.postProcess(result));
            }
        };
    }
}

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

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.utils.Pair;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * Runs two post-processed sketches concurrently.
 * @param <T>   Type of data.
 * @param <R1>  First sketch result type.
 * @param <R2>  Second sketch result type.
 * @param <F1>  First post-processed result type.
 * @param <F2>  Second post-processed result type.
 */
public class ConcurrentPostprocessedSketch<T, R1 extends ISketchResult, R2 extends ISketchResult,
                                            F1 extends IJson, F2 extends IJson>
        extends PostProcessedSketch<T, Pair<R1, R2>, Pair<F1, F2>> {
    private final PostProcessedSketch<T, R1, F1> first;
    private final PostProcessedSketch<T, R2, F2> second;

    /**
     * Create a ConcurrentPostProcessed sketch.
     * @param first    First post-processed sketch to run.
     * @param second   Second post-processed sketch to run.
     */
    public ConcurrentPostprocessedSketch(PostProcessedSketch<T, R1, F1> first,
                                         PostProcessedSketch<T, R2, F2> second) {
        super(new ConcurrentSketch<T, R1, R2>(first.sketch, second.sketch));
        this.first = first;
        this.second = second;
    }

    @Nullable
    @Override
    public Pair<F1, F2> postProcess(@Nullable Pair<R1, R2> result) {
        Converters.checkNull(result);
        F1 f1 = this.first.postProcess(result.first);
        F2 f2 = this.second.postProcess(result.second);
        return new Pair<F1, F2>(f1, f2);
    }
}

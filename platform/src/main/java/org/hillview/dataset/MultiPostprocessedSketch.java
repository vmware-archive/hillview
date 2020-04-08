/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.dataset;

import org.hillview.dataset.api.IJson;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Runs multiple post-processed sketches concurrently; returns a JsonList with all results.
 * @param <T>   Type of data.
 * @param <R>   Sketch result type.
 * @param <F>   Post-processed result type.
 */
public class MultiPostprocessedSketch<T, R extends Serializable, F extends IJson>
        extends PostProcessedSketch<T, ArrayList<R>, JsonList<F>> {

    private final List<Function<R, F>> post;

    public MultiPostprocessedSketch(List<PostProcessedSketch<T, R, F>> sketches) {
        super(new MultiSketch<T, R>(Linq.map(sketches, s -> s.sketch)));
        this.post = Linq.map(sketches, s -> s::postProcess);
    }

    @SafeVarargs
    public MultiPostprocessedSketch(PostProcessedSketch<T, R, F>... sketches) {
        super(new MultiSketch<T, R>(Linq.map(Arrays.asList(sketches), s -> s.sketch)));
        this.post = Linq.map(Arrays.asList(sketches), s -> s::postProcess);
    }

    @Nullable
    @Override
    public JsonList<F> postProcess(@Nullable ArrayList<R> result) {
        Converters.checkNull(result);
        return new JsonList<F>(Linq.map(Linq.zip(result, post),
                p -> Converters.checkNull(p.second).apply(p.first)));
    }
}

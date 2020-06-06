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

import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.dataset.api.ISketch;

import javax.annotation.Nullable;

/**
 * A sketch with post processing that applies the identify function for post processing.
 * @param <T>   Input sketch data.
 * @param <R>   Output sketch data.
 */
public class IdPostProcessedSketch<T, R extends IJsonSketchResult> extends PostProcessedSketch<T, R, R> {
    public IdPostProcessedSketch(ISketch<T, R> sketch) {
        super(sketch);
    }

    @Override
    @Nullable
    public R postProcess(@Nullable R data) { return data; }
}

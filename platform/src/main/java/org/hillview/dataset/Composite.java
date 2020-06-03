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

package org.hillview.dataset;

import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.ISketchResult;

import java.io.Serializable;

import javax.annotation.Nullable;

public class Composite<T, S, R extends ISketchResult> implements ISketch<T, R> {
    static final long serialVersionUID = 1;

    private final IMap<T, S> map;
    private final ISketch<S, R> sketch;

    public Composite(IMap<T, S> map, ISketch<S, R> sketch) {
        this.map = map;
        this.sketch = sketch;
    }

    @Nullable
    @Override
    public R zero() {
        return this.sketch.zero();
    }

    @Nullable
    @Override
    public R add(@Nullable R left, @Nullable R right) {
        return this.sketch.add(left, right);
    }

    @Override
    public R create(@Nullable T data) {
        S first = this.map.apply(data);
        return this.sketch.create(first);
    }
}

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
 *
 */

package org.hiero.sketch.dataset;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.Pair;

import javax.annotation.Nullable;

public class ConcurrentSketch<T, R1, R2> implements ISketch<T, Pair<R1, R2>> {
    final ISketch<T, R1> first;
    final ISketch<T, R2> second;

    public ConcurrentSketch(ISketch<T, R1> first, ISketch<T, R2> second) {
        this.first = first;
        this.second = second;
    }

    @Nullable
    @Override
    public Pair<R1, R2> zero() {
        return new Pair<R1, R2>(this.first.zero(), this.second.zero());
    }

    @Nullable
    @Override
    public Pair<R1, R2> add(@Nullable Pair<R1, R2> left, @Nullable Pair<R1, R2> right) {
        R1 first = this.first.add(left.first, right.first);
        R2 second = this.second.add(left.second, right.second);
        return new Pair<R1, R2>(first, second);
    }

    @Override
    public Pair<R1, R2> create(T data) {
        R1 first = this.first.create(data);
        R2 second = this.second.create(data);
        return new Pair<R1, R2>(first, second);
    }
}

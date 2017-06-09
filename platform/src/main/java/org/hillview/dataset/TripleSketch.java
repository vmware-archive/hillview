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

package org.hillview.dataset;

import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Triple;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class TripleSketch<T, R1, R2, R3> implements ISketch<T, Triple<R1, R2, R3>> {
    final ISketch<T, R1> first;
    final ISketch<T, R2> second;
    final ISketch<T, R3> third;

    public TripleSketch(ISketch<T, R1> first, ISketch<T, R2> second, ISketch<T, R3> third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Nullable
    @Override
    public Triple<R1, R2, R3> zero() {
        return new Triple<R1, R2, R3>(
                this.first.zero(), this.second.zero(), this.third.zero());
    }

    @Nullable
    @Override
    public Triple<R1, R2, R3> add(@Nullable Triple<R1, R2, R3> left, @Nullable Triple<R1, R2, R3> right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        R1 first = this.first.add(left.first, right.first);
        R2 second = this.second.add(left.second, right.second);
        R3 third = this.third.add(left.third, right.third);
        return new Triple<R1, R2, R3>(first, second, third);
    }

    @Override
    public Triple<R1, R2, R3> create(T data) {
        R1 first = this.first.create(data);
        R2 second = this.second.create(data);
        R3 third = this.third.create(data);
        return new Triple<R1, R2, R3>(first, second, third);
    }
}

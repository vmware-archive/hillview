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

import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;

import java.io.Serializable;

import javax.annotation.Nullable;

public class ConcurrentSketch<T, R1 extends Serializable, R2 extends Serializable> 
                                    implements ISketch<T, Pair<R1, R2>> {
    static final long serialVersionUID = 1;
    
    private final ISketch<T, R1> first;
    private final ISketch<T, R2> second;

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
        assert left != null;
        assert right != null;
        R1 first = this.first.add(left.first, right.first);
        R2 second = this.second.add(left.second, right.second);
        return new Pair<R1, R2>(first, second);
    }

    @Override
    public Pair<R1, R2> create(@Nullable T data) {
        R1 first = this.first.create(data);
        R2 second = this.second.create(data);
        return new Pair<R1, R2>(first, second);
    }
}

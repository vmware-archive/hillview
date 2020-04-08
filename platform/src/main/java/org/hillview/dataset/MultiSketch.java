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

package org.hillview.dataset;

import org.hillview.dataset.api.IMonoid;
import org.hillview.dataset.api.ISketch;
import org.hillview.utils.Converters;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs multiple sketches of the same type.
 * @param <T>  Input data type.
 * @param <R>  Output sketch type.
 */
public class MultiSketch<T, R extends Serializable>
                            implements ISketch<T, ArrayList<R>> {
    static final long serialVersionUID = 1;
    private final List<ISketch<T, R>> sketches;

    public MultiSketch(List<ISketch<T, R>> sketches) {
        this.sketches = sketches;
    }

    public MultiSketch(ISketch<T, R>... sketches) {
        this.sketches = Arrays.asList(sketches);
    }

    @Nullable
    @Override
    public ArrayList<R> zero() {
        return Linq.map(this.sketches, IMonoid::zero);
    }

    @Nullable
    @Override
    public ArrayList<R> add(@Nullable ArrayList<R> left, @Nullable ArrayList<R> right) {
        assert left != null;
        assert right != null;
        return Linq.map(Linq.zip3(left, right, this.sketches),
                t -> Converters.checkNull(t.third).add(t.first, t.second));
    }

    @Override
    public ArrayList<R> create(@Nullable T data) {
        return Linq.map(this.sketches, s -> s.create(data));
    }
}

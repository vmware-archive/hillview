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

package org.hillview.sketches;

import org.hillview.dataset.api.IncrementalTableSketch;
import org.hillview.sketches.results.Count;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A sketch which just increments every time it is invoked.
 */
public class CounterSketch extends IncrementalTableSketch<Count, EmptyWorkspace> {
    static final long serialVersionUID = 1;

    @Override
    public void add(EmptyWorkspace v, Count result, int rowNumber) {
        result.add(1);
    }

    @Override
    public EmptyWorkspace initialize(ITable data) { return EmptyWorkspace.instance; }

    @Override
    public Count rescale(Count result, double samplingRate) {
        return new Count(Converters.toLong(result.count / samplingRate));
    }

    @Nullable
    @Override
    public Count create(@Nullable ITable data) {
        int size = Converters.checkNull(data).getMembershipSet().getSize();
        return new Count(size);
    }

    @Nullable
    @Override
    public Count zero() {
        return new Count();
    }

    @Nullable
    @Override
    public Count add(@Nullable Count left, @Nullable Count right) {
        return new Count(Converters.checkNull(left).count + Converters.checkNull(right).count);
    }
}

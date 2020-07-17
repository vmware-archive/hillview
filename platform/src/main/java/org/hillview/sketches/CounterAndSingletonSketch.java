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
import org.hillview.sketches.results.CountAndSingleton;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A sketch which just increments every time it is invoked.
 */
public class CounterAndSingletonSketch extends
        IncrementalTableSketch<CountAndSingleton, VirtualRowSnapshot> {
    static final long serialVersionUID = 1;
    final Schema schema;

    public CounterAndSingletonSketch(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void increment(VirtualRowSnapshot v, CountAndSingleton result, int rowNumber) {
        result.increment(v, rowNumber);
    }

    @Override
    public VirtualRowSnapshot initialize(ITable data) {
        return new VirtualRowSnapshot(data, this.schema);
    }

    @Override
    public CountAndSingleton rescale(CountAndSingleton result, double samplingRate) {
        return result.rescale(samplingRate);
    }

    @Nullable
    @Override
    public CountAndSingleton create(@Nullable ITable data) {
        int size = Converters.checkNull(data).getMembershipSet().getSize();
        @Nullable
        RowSnapshot row = null;
        if (size == 1)
            row = new RowSnapshot(data, 0);
        return new CountAndSingleton(size, row);
    }

    @Nullable
    @Override
    public CountAndSingleton zero() {
        return new CountAndSingleton();
    }

    @Nullable
    @Override
    public CountAndSingleton add(@Nullable CountAndSingleton left, @Nullable CountAndSingleton right) {
        return Converters.checkNull(left).add(Converters.checkNull(right));
    }
}

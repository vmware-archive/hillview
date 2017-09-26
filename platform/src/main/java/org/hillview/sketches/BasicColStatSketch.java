/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import javax.annotation.Nullable;

public class BasicColStatSketch implements ISketch<ITable, BasicColStats> {
    private final ColumnNameAndConverter col;
    private final double rate;
    private final int momentNum;

    public BasicColStatSketch(ColumnNameAndConverter col) {
        this.col = col;
        this.rate = 1;
        this.momentNum = 2;
    }

    public BasicColStatSketch(ColumnNameAndConverter col, int momentNum, double rate) {
        this.col = col;
        this.rate = rate;
        this.momentNum = momentNum;
    }

    @Override
    public BasicColStats create(final ITable data) {
        BasicColStats result = this.getZero();
        result.createStats(data.getColumn(this.col), data.getMembershipSet().sample(this.rate));
        return result;
    }

    @Override
    public BasicColStats zero() { return new BasicColStats(this.momentNum); }

    @Override
    public BasicColStats add(@Nullable final BasicColStats left, @Nullable final BasicColStats right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}

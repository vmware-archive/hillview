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

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.Randomness;

import javax.annotation.Nullable;

public class HLogLogSketch implements ISketch<ITable, HLogLog> {
    private final String colName;
    private final long seed; //seed for the hash function of the HLogLog
    /**
     * the log of the #bytes used by each data structure. Should be in 4...16.
     * More space means more accuracy. A space of 10-14 is recommended.
     **/
    private final int logSpaceSize;

    public HLogLogSketch(String colName) {
        this.colName = colName;
        this.seed = new Randomness().nextLong();
        this.logSpaceSize = 12;
    }

    public HLogLogSketch(String colName, int logSpaceSize, long seed) {
        this.colName = colName;
        this.seed = seed;
        HLogLog.checkSpaceValid(logSpaceSize);
        this.logSpaceSize = logSpaceSize;
    }

    @Override
    public HLogLog create(final ITable data) {
        HLogLog result = this.getZero();
        ColumnAndConverterDescription ccd = new ColumnAndConverterDescription(this.colName);
        ColumnAndConverter col = data.getLoadedColumn(ccd);
        result.createHLL(col.column, data.getMembershipSet());
        return result;
    }

    @Override
    public HLogLog add(@Nullable final HLogLog left, @Nullable final HLogLog right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public HLogLog zero() {
        return new HLogLog(this.logSpaceSize, this.seed);
    }
}

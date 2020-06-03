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

import org.hillview.dataset.TableSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.HLogLog;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.QuantizedColumn;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class HLogLogSketch implements TableSketch<HLogLog> {
    static final long serialVersionUID = 1;

    private final String colName;
    private final long seed; //seed for the hash function of the HLogLog
    /**
     * the log of the #bytes used by each data structure. Should be in 4...16.
     * More space means more accuracy. A space of 10-14 is recommended.
     **/
    private final int logSpaceSize;
    @Nullable
    private final ColumnQuantization quantization;

    public HLogLogSketch(String colName, long seed) {
        this(colName, 12, seed, null);
    }

    public HLogLogSketch(String colName, int logSpaceSize, long seed,
                  @Nullable ColumnQuantization quantization) {
        this.colName = colName;
        this.seed = seed;
        HLogLog.checkSpaceValid(logSpaceSize);
        this.logSpaceSize = logSpaceSize;
        this.quantization = quantization;
    }

    @Override
    public HLogLog create(@Nullable final ITable data) {
        HLogLog result = this.getZero();
        IColumn col = Converters.checkNull(data).getLoadedColumn(this.colName);
        if (this.quantization != null)
            col = new QuantizedColumn(col, this.quantization);
        Converters.checkNull(result).createHLL(col, data.getMembershipSet());
        return result;
    }

    @Override
    public HLogLog add(@Nullable final HLogLog left, @Nullable final HLogLog right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }

    @Override
    public HLogLog zero() {
        return new HLogLog(this.logSpaceSize, this.seed);
    }
}

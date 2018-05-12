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
import org.hillview.table.ArrayRowOrder;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowOrder;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.hillview.table.columns.ObjectArrayColumn.mergeColumns;

public class SampleQuantileSketch  implements ISketch<ITable, SampleList> {
    private final RecordOrder colSortOrder;
    /**
     * The rate at which we sample the data. It is set to be (resolution)^2/dataSize,
     * so we expect a table of size (resolution)^2. This is chosen so that the desired accuracy in
     * the quantiles is 1/resolution. It can be tuned by changing the size by a constant factor.
     * For a resolution of 100, the expected sample size is 10,000.
     */
    private final double samplingRate;
    private final long seed;

    /**
     * @param sortOrder The sorting order on the columns.
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     * @param dataSize The size of the input table on which we want to run the quantile computation.
     */
    public SampleQuantileSketch(final RecordOrder sortOrder, final int resolution,
                                final long dataSize, final long seed) {
        this.colSortOrder = sortOrder;
        double n = Math.max(resolution, 100);
        this.samplingRate = (n * n)/dataSize;
        this.seed = seed;
    }

    @Nullable
    @Override
    public SampleList zero() {
        return new SampleList(new SmallTable(this.colSortOrder.toSchema()));
    }

    /**
     * Creates a table by sampling with probability samplingRate and then sorting by sortOrder.
     * @param data  Data to sketch.
     * @return A table with samples sorted, and columns compressed to the relevant ones.
     */
    @Override
    public SampleList create(ITable data) {
        final IMembershipSet sampleSet = data.getMembershipSet().sample(
                this.samplingRate, this.seed);
        final IRowOrder rowOrder = new ArrayRowOrder(this.
                colSortOrder.getSortedRowOrder(data, sampleSet));
        return new SampleList(data.compress(this.colSortOrder.toSubSchema(), rowOrder));
    }

    /**
     * Merges two sample tables with the ordering specified by sortOrder.
     */
    @Override
    public SampleList add(@Nullable SampleList left, @Nullable SampleList right) {
        assert left != null;
        assert right != null;
        if (!left.table.getSchema().equals(right.table.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        List<IColumn> mergedCol = new ArrayList<IColumn>(left.table.getSchema().getColumnCount());
        boolean[] mergeLeft = this.colSortOrder.getMergeOrder(left.table, right.table);
        for (String colName: left.table.getSchema().getColumnNames()) {
            IColumn newCol = mergeColumns(left.table.getColumn(colName),
                    right.table.getColumn(colName), mergeLeft);
            mergedCol.add(newCol);
        }
        return new SampleList(new SmallTable(mergedCol));
    }
}
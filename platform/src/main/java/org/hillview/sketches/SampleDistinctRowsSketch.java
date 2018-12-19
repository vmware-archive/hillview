/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import net.openhft.hashing.LongHashFunction;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.ArrayRowOrder;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.*;
import org.hillview.table.columns.ObjectArrayColumn;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SampleDistinctRowsSketch implements ISketch<ITable, DistinctKList> {

    private final RecordOrder recordOrder;
    @Nullable
    private final RowSnapshot topRow;
    private final int maxSize;
    private final long seed;

    public SampleDistinctRowsSketch(RecordOrder recordOrder, RowSnapshot topRow, int maxSize, long seed) {
        this.recordOrder = recordOrder;
        this.topRow = topRow;
        this.maxSize = maxSize;
        this.seed = seed;
    }

    @Override
    public DistinctKList create(ITable data) {
        IndexComparator comp = this.recordOrder.getComparator(data);
        IntTreeTopK iTopK = new IntTreeTopK(this.maxSize, comp);
        Schema toBring = this.recordOrder.toSchema();
        IRowIterator rowIt = data.getRowIterator();
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data, toBring);
        LongHashFunction hash = LongHashFunction.xx(this.seed);
        double threshold = Math.min(1, this.width/ ((double) this.universeSize));
        for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
            vw.setRow(i);
            if (Math.abs(hash.hashLong(vw.hashCode())) < threshold * Long.MAX_VALUE) {
                if (this.topRow == null)
                    iTopK.push(i);
                else if (this.topRow.compareTo(vw, this.recordOrder) <= 0)
                    iTopK.push(i);
            }
        }
        Int2IntSortedMap topKList = iTopK.getTopK();
        IRowOrder rowOrder = new ArrayRowOrder(topKList.keySet().toIntArray());
        SmallTable topKRows = data.compress(this.recordOrder.toSchema(), rowOrder);
        return new DistinctKList(topKRows, maxSize);
    }


    @Override
    public DistinctKList zero() {
        return new DistinctKList(this.recordOrder.toSchema(), this.maxSize);
    }


    @Nullable
    public DistinctKList add(@Nullable DistinctKList left, @Nullable DistinctKList right) {
        assert left != null;
        assert right != null;
        if (!left.table.getSchema().equals(right.table.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        int cols = left.table.getSchema().getColumnCount();
        List<IColumn> mergedCol = new ArrayList<IColumn>(cols);
        List<Integer> mergeOrder = this.recordOrder.getIntMergeOrder(left.table, right.table);
        for (String colName : left.table.getSchema().getColumnNames()) {
            IColumn newCol = ObjectArrayColumn.mergeColumns(left.table.getColumn(colName),
                    right.table.getColumn(colName), mergeOrder, this.maxSize);
            mergedCol.add(newCol);
        }
        final SmallTable mergedTable = new SmallTable(mergedCol);
        return new DistinctKList(mergedTable, this.maxSize);
    }
}

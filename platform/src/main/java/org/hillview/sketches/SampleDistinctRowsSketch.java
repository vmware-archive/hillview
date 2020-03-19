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

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import net.openhft.hashing.LongHashFunction;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.MinKRows;
import org.hillview.sketches.results.MinKSet;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.Comparator;


/**
 * This sketch produces a uniformly random sample of (numSample many) distinct rows from a table
 * (assuming truly random hashing). If there are fewer than numSample distinct rows, then all of
 * them are returned. In addition, we also return the minimum and maximum values, according to a
 * specified ordering of the rows.
 */
public class SampleDistinctRowsSketch implements ISketch<ITable, MinKSet<RowSnapshot>> {
    static final long serialVersionUID = 1;
    
    private final RecordOrder recordOrder;
    private final int numSamples;
    private final long seed;

    public SampleDistinctRowsSketch(RecordOrder recordOrder, int numSamples, long seed) {
        this.recordOrder = recordOrder;
        this.numSamples = numSamples;
        this.seed = seed;
    }

    @Override
    public MinKSet<RowSnapshot> create(@Nullable ITable data) {
        IndexComparator comp = this.recordOrder.getIndexComparator(Converters.checkNull(data));
        Schema schema = this.recordOrder.toSchema();
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data, schema);
        MinKRows mkRows = new MinKRows(numSamples);
        LongHashFunction hash = LongHashFunction.xx(this.seed);
        IRowIterator rowIt = data.getRowIterator();
        int currRow = rowIt.getNextRow();
        int maxRow, minRow;
        if (currRow == -1)
            return this.zero();
        else {
            minRow = currRow;
            maxRow = currRow;
        }
        while (currRow != -1) {
            vw.setRow(currRow);
            mkRows.push(hash.hashLong(vw.hashCode()), currRow);
            if (comp.compare(minRow, currRow) > 0)
                minRow = currRow;
            if (comp.compare(maxRow, currRow) < 0)
                maxRow = currRow;
            currRow = rowIt.getNextRow();
        }
        Long2ObjectRBTreeMap<RowSnapshot> hMap = new Long2ObjectRBTreeMap<RowSnapshot>();
        for (long hashKey: mkRows.hashMap.keySet())
            hMap.put(hashKey, new RowSnapshot(data, mkRows.hashMap.get(hashKey), schema));
        RowSnapshot minRS = new RowSnapshot(data, minRow, schema);
        RowSnapshot maxRS = new RowSnapshot(data, maxRow, schema);
        return new MinKSet<RowSnapshot>(numSamples, hMap, this.recordOrder.getRowComparator(),
                minRS, maxRS, data.getNumOfRows(), 0 );
    }

    @Override
    public MinKSet<RowSnapshot> zero() {
        return new MinKSet<RowSnapshot>(this.numSamples, this.recordOrder.getRowComparator());
    }

    @Nullable
    public MinKSet<RowSnapshot> add(@Nullable MinKSet<RowSnapshot>left,
                                    @Nullable MinKSet<RowSnapshot> right) {
        assert left != null;
        assert right != null;
        Comparator<RowSnapshot> comp = left.comp;
        RowSnapshot minRS, maxRS;
        long present = left.presentCount + right.presentCount;
        if (left.presentCount == 0) {
            minRS = right.min;
            maxRS = right.max;
        } else if (right.presentCount == 0) {
            minRS = right.min;
            maxRS = right.max;
        } else {
            minRS = comp.compare(left.min, right.min) < 0 ? left.min : right.min;
            maxRS = comp.compare(left.max, right.max) > 0 ? left.max : right.max;
        }
        Long2ObjectRBTreeMap<RowSnapshot> data = new Long2ObjectRBTreeMap<>();
        data.putAll(left.data);
        data.putAll(right.data);
        while (data.size() > this.numSamples) {
            long maxKey = data.lastLongKey();
            data.remove(maxKey);
        }
        return new MinKSet<RowSnapshot>(this.numSamples, data, comp, minRS, maxRS, present, 0);
    }
}

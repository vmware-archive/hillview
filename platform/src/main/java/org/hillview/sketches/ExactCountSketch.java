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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowHashStrategy;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.MutableInteger;

import javax.annotation.Nullable;
import java.util.Arrays;

public class ExactCountSketch implements ISketch<ITable, FreqKList> {
    public CountSketchResult result;
    public double threshold;
    private final double cutoff;

    public ExactCountSketch(CountSketchResult result, double threshold) {
        this.result = result;
        this.threshold = threshold;
        this.cutoff =  this.threshold*this.result.estimateNorm();
    }

    @Override
    public FreqKList create(ITable data) {
        VirtualRowHashStrategy hashStrategy = new VirtualRowHashStrategy(data,
                this.result.csDesc.schema);
        Int2ObjectOpenCustomHashMap<MutableInteger> hMap =
                new Int2ObjectOpenCustomHashMap<MutableInteger>(hashStrategy);
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, result.csDesc.schema);
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        MutableInteger val;
        long item, hash;
        int sign, toBucket;
        long[] estimate = new long[this.result.csDesc.trials];
        while (i != -1) {
            val = hMap.get(i);
            if (val != null) {
                val.set(val.get() + 1);
            } else {
                vrs.setRow(i);
                item = vrs.hashCode();
                for (int j = 0; j < this.result.csDesc.trials; j++) {
                    hash = this.result.csDesc.hashFunction[j].hashLong(item);
                    sign = (int) hash & 1;
                    toBucket = (int) (Math.abs(hash / 2) % this.result.csDesc.buckets);
                    estimate[j] = this.result.counts[j][toBucket] * sign;
                }
                Arrays.sort(estimate);
                if (estimate[this.result.csDesc.trials/2] > this.cutoff)
                    hMap.put(i, new MutableInteger(1));
            }
            i = rowIt.getNextRow();
        }
        Object2IntOpenHashMap<RowSnapshot> hm =  hashStrategy.materializeHashMap(hMap);
        for (RowSnapshot rss : hm.keySet())
            if (hm.getInt(rss) < this.cutoff)
                hm.remove(rss, hm.getInt(rss));
        FreqKList fkList = new FreqKList(data.getNumOfRows(), threshold, hm);
        return fkList;
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(0, this.threshold, new Object2IntOpenHashMap<>());
    }

    @Nullable
    @Override
    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        FreqKList fkList =  new FreqKList(left.totalRows + right.totalRows, this.threshold,
                FreqKList.getUnion(left, right));
        return fkList;
    }
}

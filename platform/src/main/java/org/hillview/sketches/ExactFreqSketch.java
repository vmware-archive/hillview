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

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.rows.BaseRowSnapshot;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.Schema;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;

/**
 * This sketch computes the true frequencies of a list of rowSnapshots in a data set. It can
 * be used right after the FreqKSketch which computes the list of heavy hitters, to compute their
 * exact frequencies.
 */
public class ExactFreqSketch implements ISketch<ITable, FreqKList> {

    /**
     * The schema of the RowSnapshots
     */
    private final Schema schema;
    /**
     * The set of RowSnapshots whose frequencies we wish to compute.
     */
    private final List<RowSnapshot> rssList;
    /**
     * The K in top top-K. Is used as a threshold to eliminate items that do not occur with
     * frequency 1/K.
     */
    private int maxSize;
    private final double epsilon;

    public ExactFreqSketch(Schema schema, FreqKList fk) {
        this.schema = schema;
        this.rssList = fk.getList();
        this.epsilon = fk.epsilon;
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(this.rssList, this.epsilon);
    }

    @Override
    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        Converters.checkNull(left);
        Converters.checkNull(right);
        return left.add(right);
    }

    @Override
    public FreqKList create(ITable data) {
        HashingStrategy<BaseRowSnapshot> hs = new HashingStrategy<BaseRowSnapshot>() {
            @Override
            public int computeHashCode(BaseRowSnapshot brs) {
                if (brs instanceof VirtualRowSnapshot) {
                    return brs.hashCode();
                } else if (brs instanceof RowSnapshot) {
                    return brs.computeHashCode(ExactFreqSketch.this.schema);
                } else throw new RuntimeException("Uknown type encountered");
            }

            @Override
            public boolean equals(BaseRowSnapshot brs1, BaseRowSnapshot brs2) {
                return brs1.compareForEquality(brs2, ExactFreqSketch.this.schema);
            }
        };
        UnifiedMapWithHashingStrategy<BaseRowSnapshot, Integer> hMap = new
                UnifiedMapWithHashingStrategy<BaseRowSnapshot, Integer>(hs);
        this.rssList.forEach(rss -> hMap.put(rss, 0));
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.schema);
        while (i != -1) {
            vrs.setRow(i);
            if (hMap.containsKey(vrs)) {
                int count = hMap.get(vrs);
                hMap.put(vrs, count + 1);
            }
            i = rowIt.getNextRow();
        }
        HashMap<RowSnapshot, Integer> hm = new HashMap<RowSnapshot, Integer>(this.rssList.size());
        this.rssList.forEach(rss -> hm.put(rss, hMap.get(rss)));
        return new FreqKList(data.getNumOfRows(), this.epsilon, hm);
    }
}

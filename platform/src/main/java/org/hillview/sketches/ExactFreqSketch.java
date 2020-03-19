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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.FreqKList;
import org.hillview.sketches.results.FreqKListExact;
import org.hillview.table.Schema;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.BaseRowSnapshot;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This sketch computes the true frequencies of a list of rowSnapshots in a data set. It can
 * be used right after any approximate sketch for Heavy Hitters to compute their exact frequencies.
 */
public class ExactFreqSketch implements ISketch<ITable, FreqKListExact> {
    static final long serialVersionUID = 1;
    /**
     * The schema of the RowSnapshots
     */
    private final Schema schema;
    /**
     * The set of RowSnapshots whose frequencies we wish to compute.
     */
    private final List<RowSnapshot> rssList;
    private final double epsilon;

    public ExactFreqSketch(Schema schema, FreqKList fk) {
        this.schema = schema;
        this.rssList = fk.getList();
        this.epsilon = fk.epsilon;
    }

    @Nullable
    @Override
    public FreqKListExact zero() {
        return new FreqKListExact(this.epsilon, this.rssList);
    }

    /**
     * Adds the frequencies for each row from the two lists.
     */
    @Override
    public FreqKListExact add(@Nullable FreqKListExact left, @Nullable FreqKListExact right) {
        final FreqKListExact l = Converters.checkNull(left);
        final FreqKListExact r = Converters.checkNull(right);
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(this.rssList.size());
        this.rssList.forEach(rss -> hm.put(
                rss, l.hMap.getInt(rss) + r.hMap.getInt(rss)));
        return new FreqKListExact(l.totalRows + r.totalRows,
                this.epsilon, hm, this.rssList);
    }

    /**
     * Compute frequency for each RowSnapShot over a table.
     */
    @Override
    public FreqKListExact create(@Nullable ITable data) {
        Converters.checkNull(data).getColumns(this.schema);
        Hash.Strategy<BaseRowSnapshot> hs = new Hash.Strategy<BaseRowSnapshot>() {
            @Override
            public int hashCode(BaseRowSnapshot brs) {
                if (brs instanceof VirtualRowSnapshot) {
                    return brs.hashCode();
                } else if (brs instanceof RowSnapshot) {
                    return brs.computeHashCode(ExactFreqSketch.this.schema);
                } else throw new RuntimeException("Uknown type encountered");
            }

            @Override
            public boolean equals(BaseRowSnapshot brs1, @Nullable BaseRowSnapshot brs2) {
                // brs2 is null because the hashmap explicitly calls with null
                // even if null cannot be a key.
                if (brs2 == null)
                    return brs1 == null;
                return brs1.compareForEquality(brs2, ExactFreqSketch.this.schema);
            }
        };
        Object2IntMap<BaseRowSnapshot> hMap = new Object2IntOpenCustomHashMap<BaseRowSnapshot>(hs);
        this.rssList.forEach(rss -> hMap.put(rss, 0));
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.schema);
        while (i != -1) {
            vrs.setRow(i);
            if (hMap.containsKey(vrs)) {
                int count = hMap.getInt(vrs);
                hMap.put(vrs, count + 1);
            }
            i = rowIt.getNextRow();
        }
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(this.rssList.size());
        this.rssList.forEach(rss -> hm.put(rss, hMap.getInt(rss)));
        return new FreqKListExact(data.getNumOfRows(), this.epsilon, hm, this.rssList);
    }
}

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

package org.hillview.table.rows;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.MutableInteger;

/**
 * This class implements hashing for integers based on a table of data and a schema. It treats each
 * integer as a rowIndex into the table and declares two integers equal if the RowSnapShots for the
 * specified schema agree.
 */

public class VirtualRowHashStrategy implements IntHash.Strategy {
    private final Schema schema;
    private final ITable data;
    private final VirtualRowSnapshot vrs;
    private final VirtualRowSnapshot vrsToCompare;

    public VirtualRowHashStrategy(ITable data, Schema schema) {
        this.data = data;
        this.schema = schema;
        this.vrsToCompare = new VirtualRowSnapshot(data, schema);
        this.vrs = new VirtualRowSnapshot(data, schema);
    }

    @Override
    public int hashCode(int index) {
        this.vrs.setRow(index);
        return this.vrs.computeHashCode(schema);
    }

    @Override
    public boolean equals(int index, int otherIndex) {
        this.vrs.setRow(index);
        this.vrsToCompare.setRow(otherIndex);
        return this.vrs.compareForEquality(this.vrsToCompare, this.schema);
    }

    /**
     * @param hMap: a hashMap of integers (which point to rows in a table) and their counts
     *            typically represented by MutableInt objects for efficiency.
     * @return a new hashMap with RowSnapShots and their integer counts. This is serializable.
     */
    public Object2IntOpenHashMap<RowSnapshot>
    materializeHashMap(Int2ObjectOpenCustomHashMap<MutableInteger> hMap) {
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(hMap.size());
        for (ObjectIterator<Int2ObjectMap.Entry<MutableInteger>>
             it = hMap.int2ObjectEntrySet().fastIterator();
             it.hasNext(); ) {
            final Int2ObjectMap.Entry<MutableInteger> entry = it.next();
            hm.put(new RowSnapshot(this.data, entry.getIntKey(), this.schema),
                    entry.getValue().get());
        }
        return hm;
    }
}

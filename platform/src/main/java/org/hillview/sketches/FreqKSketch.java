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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.Schema;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.MutableInteger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;

/** Computes heavy-hitters using the Misra-Gries algorithm, where N is the length on the input
 * table, and our goal is find all elements of frequency epsilon N. K is the number of counters
 * that we maintain, we set it to alpha/epsilon. Increasing alpha increases accuracy of the counts.
 * We use the mergeable version of MG, as described in the ACM TODS paper "Mergeable Summaries" by
 * Agarwal et al., which gives a non-trivial error bound. The algorithm ensures that every element
 * of frequency greater than N/(k+1) appears in the list.
 */
public class FreqKSketch implements ISketch<ITable, FreqKList> {
    /**
     * The schema specifies which columns are relevant in determining equality of records.
     */
    private final Schema schema;

    /**
     * epsilon specifies the threshold for the fractional frequency: our goal is to find all elements
     * that constitute more than an epsilon fraction of the total.
     */
    private final double epsilon;

    /**
     * This controls the relative error in the counts returned by MG.
     * The relative error goes down as 1/alpha. More precisely, for every element whose true
     * fractional frequency is eps, the reported frequency lies between eps and eps(1 - 1/alpha).
     */
    private static final int alpha = 5;

    /**
     * The parameter K which controls how many indices we store in MG. We set it to alpha/epsilon.
     */
    private final int maxSize;

    public FreqKSketch(Schema schema, double epsilon) {
        this.schema = schema;
        this.epsilon = epsilon;
        this.maxSize = ((int) Math.ceil(alpha/epsilon));
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(0, this.epsilon, this.maxSize, new Object2IntOpenHashMap<RowSnapshot>(0));
    }

    /**
     * The add procedure as specified by Agarwal et al. (Mergeable Summaries, TODS).
     * @param left The first MG sketch.
     * @param right The second MG sketch.
     * @return The merged sketch, where we first add the frequency vectors, and then subtract the
     * (k+1)^th frequency from the top k. This guarantees a strong error bound.
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public FreqKList add(@Nullable FreqKList left, @Nullable FreqKList right) {
        Object2ObjectOpenHashMap<RowSnapshot, MutableInteger> resultMap =
                new Object2ObjectOpenHashMap<RowSnapshot, MutableInteger>(left.hMap.size());
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it1 = left.hMap.object2IntEntrySet().fastIterator();
             it1.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> it = it1.next();
            resultMap.put(it.getKey(), new MutableInteger(it.getIntValue()));
        }

        // Add values of right.hMap to resultMap
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it1 = right.hMap.object2IntEntrySet().fastIterator();
             it1.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> it = it1.next();
            MutableInteger val = resultMap.get(it.getKey());
            if (val != null) {
                val.set(val.get() + it.getIntValue());
            }
            else {
                resultMap.put(it.getKey(), new MutableInteger(it.getIntValue()));
            }
        }

        List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> pList =
                new ArrayList<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>>(resultMap.size());
        pList.addAll(resultMap.object2ObjectEntrySet());
        pList.sort((p1, p2) -> Integer.compare(p2.getValue().get(), p1.getValue().get()));

        int k = 0;
        if (pList.size() >= (this.maxSize + 1))
            k = pList.get(this.maxSize).getValue().get();
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(this.maxSize);
        for (int i = 0; i < Math.min(this.maxSize, pList.size()); i++) {
            if (pList.get(i).getValue().get() >= (k + 1))
                hm.put(pList.get(i).getKey(), pList.get(i).getValue().get() - k);
        }
        return new FreqKList(left.totalRows + right.totalRows, this.epsilon, this.maxSize, hm);
    }

    /**
     * Creates the MG sketch, by the Misra-Gries algorithm.
     * @param data  Data to sketch.
     * @return A FreqKList.
     */
    @Override
    public FreqKList create(ITable data) {
        IRowIterator rowIt = data.getRowIterator();
        IntHash.Strategy hs = new IntHash.Strategy() {
            final VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, FreqKSketch.this.schema);
            final VirtualRowSnapshot vrs1 = new VirtualRowSnapshot(data, FreqKSketch.this.schema);

            @Override
            public int hashCode(int index) {
                this.vrs.setRow(index);
                return this.vrs.computeHashCode(FreqKSketch.this.schema);
            }

            @Override
            public boolean equals(int index, int otherIndex) {
                this.vrs.setRow(index);
                this.vrs1.setRow(otherIndex);
                return this.vrs.compareForEquality(this.vrs1, FreqKSketch.this.schema);
            }
        };

        Int2ObjectOpenCustomHashMap<MutableInteger> hMap = new Int2ObjectOpenCustomHashMap<MutableInteger>(hs);
        IntSet toRemove = new IntOpenHashSet(this.maxSize);
        int i = rowIt.getNextRow();
        /* An optimization to speed up the algorithm is that we batch the decrements together in
        variable dec. We only perform an actual decrement when the total decrements equal the minimum
        count among the counts we are currently storing.*/
        int min = 0; // Minimum count currently in the hashMap
        int dec = 0; // Accumulated decrements. Should always be less than min.
        while (i != -1) {
            MutableInteger val = hMap.get(i);
            if (val != null) {
                val.set(val.get() + 1);
                if (val.get() == min)
                    min = Collections.min(hMap.values(), MutableInteger.COMPARATOR).get();
            } else if (hMap.size() < this.maxSize) {
                hMap.put(i, new MutableInteger(1));
                min = 1;
            } else {
                dec += 1;
                if (dec == min) {
                    toRemove.clear();
                    for (ObjectIterator<Int2ObjectMap.Entry<MutableInteger>> it =
                            hMap.int2ObjectEntrySet().fastIterator(); it.hasNext(); ) {
                        final Int2ObjectMap.Entry<MutableInteger> entry = it.next();
                        MutableInteger mutableInteger = entry.getValue();
                        int count = mutableInteger.get() - dec;
                        if (count == 0)
                            toRemove.add(entry.getIntKey());
                        else
                            mutableInteger.set(count);
                    }
                    toRemove.forEach((IntConsumer) hMap::remove);
                    min = !hMap.isEmpty() ? Collections.min(hMap.values(), MutableInteger.COMPARATOR).get() : 0;
                }
            }
            i = rowIt.getNextRow();
        }
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(this.maxSize);
        for (ObjectIterator<Int2ObjectMap.Entry<MutableInteger>> it = hMap.int2ObjectEntrySet().fastIterator();
             it.hasNext(); ) {
            final Int2ObjectMap.Entry<MutableInteger> entry = it.next();
            hm.put(new RowSnapshot(data, entry.getIntKey(), this.schema), entry.getValue().get());
        }
        return new FreqKList(data.getNumOfRows(), this.epsilon, this.maxSize, hm);
    }
}

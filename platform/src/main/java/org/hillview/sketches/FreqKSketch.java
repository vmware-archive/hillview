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
import org.hillview.dataset.api.Pair;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.Schema;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/** Computes heavy-hitters using the Misra-Gries algorithm, where N is the length on the input
 * table, and k is the number of counters that we maintain. We use the mergeable version of MG, as
 * described in the ACM TODS paper "Mergeable Summaries" by Agarwal et al., which gives a
 * non-trivial error bound. The algorithm ensures that every element of frequency greater than
 * N/(k+1) appears in the list.
 */
public class FreqKSketch implements ISketch<ITable, FreqKList> {
    /**
     * The schema specifies which columns are relevant in determining equality of records.
     */
    private final Schema schema;
    /**
     * The parameter K in top-K heavy hitters.
     */
    private final int maxSize;

    private final double epsilon;

    /**
     * This controls the relative error in the counts returned by MG.
     * The relative error goes down as 1/alpha. More precisely, for every element whose true
     * fractional frequency is eps, the reported frequency lies between eps and eps(1 - 1/alpha).
     */
    private int alpha = 5;

    public FreqKSketch(Schema schema, double epsilon) {
        this.schema = schema;
        this.epsilon = epsilon;
        this.maxSize = ((int) Math.ceil(1/epsilon))* alpha;
    }

    @Nullable
    @Override
    public FreqKList zero() {
        return new FreqKList(0, this.epsilon, this.maxSize, new HashMap<RowSnapshot, Integer>(0));
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
        HashMap<RowSnapshot, Integer> resultMap =
                new HashMap<RowSnapshot, Integer>(left.hMap);
        for (RowSnapshot rs : right.hMap.keySet()) {
            if (resultMap.containsKey(rs))
                resultMap.put(rs, left.hMap.get(rs) + right.hMap.get(rs));
            else
                resultMap.put(rs, right.hMap.get(rs));
        }
        List<Pair<RowSnapshot, Integer>> pList =
                new ArrayList<Pair<RowSnapshot, Integer>>(left.hMap.size());
        resultMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        pList.sort((p1, p2) -> Integer.compare(p2.second, p1.second));
        int k = 0;
        if (pList.size() >= (this.maxSize + 1))
            k = pList.get(this.maxSize).second;
        HashMap<RowSnapshot,Integer> hm = new HashMap<RowSnapshot, Integer>(this.maxSize);
        for (int i = 0; i < Math.min(this.maxSize, pList.size()); i++) {
            if (pList.get(i).second >= (k + 1))
                hm.put(pList.get(i).first, pList.get(i).second - k);
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
        HashingStrategy<Integer> hs = new HashingStrategy<Integer>() {
            final VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, FreqKSketch.this.schema);
            final VirtualRowSnapshot vrs1 = new VirtualRowSnapshot(data, FreqKSketch.this.schema);

            @Override
            public int computeHashCode(Integer index) {
                this.vrs.setRow(index);
                return this.vrs.computeHashCode(FreqKSketch.this.schema);
            }

            @Override
            public boolean equals(Integer index, Integer otherIndex) {
                this.vrs.setRow(index);
                this.vrs1.setRow(otherIndex);
                return this.vrs.compareForEquality(this.vrs1, FreqKSketch.this.schema);
            }
        };
        UnifiedMapWithHashingStrategy<Integer, Integer> hMap = new
                UnifiedMapWithHashingStrategy<Integer, Integer>(hs);

        List<Integer> toRemove = new ArrayList<Integer>(this.maxSize);
        int i = rowIt.getNextRow();
        /* An optimization to speed up the algorithm is that we batch the decrements together in
        variable dec. We only perform an actual decrement when the total decrements equal the minimum
        count among the counts we are currently storing.*/
        int min = 0; // Minimum count currently in the hashMap
        int dec = 0; // Accumulated decrements. Should always be less than min.
        while (i != -1) {
            if (hMap.containsKey(i)) {
                int val = hMap.get(i);
                hMap.put(i, val + 1);
                if (val == min)
                    min = Collections.min(hMap.values());
            } else if (hMap.size() < this.maxSize) {
                hMap.put(i, 1);
                min = 1;
            } else {
                dec += 1;
                if (dec == min) {
                    toRemove.clear();
                    for (Integer row : hMap.keySet()) {
                        int count = hMap.get(row) - dec;
                        if (count == 0)
                            toRemove.add(row);
                        else
                            hMap.put(row, count);
                    }
                    toRemove.forEach(hMap::remove);
                    min = ((!hMap.isEmpty()) ? Collections.min(hMap.values()) : 0);
                }
            }
            i = rowIt.getNextRow();
        }
        HashMap<RowSnapshot,Integer> hm = new HashMap<RowSnapshot, Integer>(this.maxSize);
        hMap.keySet().forEach(ri -> hm.put(new RowSnapshot(data, ri, this.schema), hMap.get(ri)));
        return new FreqKList(data.getNumOfRows(), this.epsilon, this.maxSize, hm);
    }
}

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

import it.unimi.dsi.fastutil.objects.*;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.RowSnapshotSet;
import org.hillview.utils.Converters;
import org.hillview.utils.MutableInteger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A data structure to store the K heavy hitters out of N elements.
 * It stores a hash-map which contains the elements and their counts, along with counts
 * of the size of the input and some metadata about the algorithm (K).
 */
public abstract class FreqKList implements Serializable {
    /**
     * The size of the input table.
     */
    public long totalRows;
    /**
     * The number of times each row in the above table occurs in the original DataSet
     * (can be approximate depending on the context).
     */
    public Object2IntOpenHashMap<RowSnapshot> hMap;

    public final double epsilon;

    protected FreqKList(long totalRows, double epsilon, Object2IntOpenHashMap<RowSnapshot> hMap) {
        this.totalRows = totalRows;
        this.epsilon = epsilon;
        this.hMap = hMap;
    }


    /**
     * @return Total distinct rows that are heavy hitters.
     */
    public int getDistinctRowCount() { return this.hMap.size(); }

    /**
     * @return The list of candidate heavy hitters. Used after running an approximate algorithm
     * to get candidates for computing exact counts.
     */
    public List<RowSnapshot> getList() {
        return new ArrayList<RowSnapshot>(this.hMap.keySet());
    }

    public static List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> addLists(FreqKList left, FreqKList right) {
        assert left != null;
        assert right != null;
        Object2ObjectOpenHashMap<RowSnapshot, MutableInteger> resultMap =
                new Object2ObjectOpenHashMap<RowSnapshot, MutableInteger>(left.hMap.size() + right.hMap.size());
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
            } else {
                resultMap.put(it.getKey(), new MutableInteger(it.getIntValue()));
            }
        }
        List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> pList =
                new ArrayList<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>>(resultMap.size());
        pList.addAll(resultMap.object2ObjectEntrySet());
        pList.sort((p1, p2) -> Integer.compare(p2.getValue().get(), p1.getValue().get()));
        return pList;
    }

    /**
     * Prunes the hashmap to retain only those RowSnapshots that occur with frequency above
     * a specified threshold.
     */
    public void fkFilter(double threshold) {
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it = this.hMap.object2IntEntrySet().fastIterator();
             it.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> entry = it.next();
            if (entry.getIntValue() < threshold) it.remove();
        }
    }

    /**
     * Post-processing method applied to the result of a heavy hitters sketch before displaying the
     * results. It will also discard elements that are too low in (estimated) frequency.
     * @param size: Lets us specify how many of the top items to select from the FreqKList.
     */
    public NextKList getTopK(int size, List<Pair<RowSnapshot, Integer>> pList, Schema schema) {

        pList.sort((p1, p2) -> Integer.compare(
                Converters.checkNull(p2.second),
                Converters.checkNull(p1.second)));
        int minSize = Math.min(size, pList.size());
        List<RowSnapshot> listRows = new ArrayList<RowSnapshot>(minSize);
        List<Integer> listCounts = new ArrayList<Integer>(minSize);
        for (int i = 0; i < minSize; i++) {
            listRows.add(pList.get(i).first);
            listCounts.add(pList.get(i).second);
        }
        return new NextKList(listRows, listCounts, schema, this.totalRows);
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public String toString() {
        List<Pair<RowSnapshot, Integer>> pList = new
                ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
        this.hMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        pList.sort((p1, p2) -> Integer.compare(p2.second, p1.second));
        final StringBuilder builder = new StringBuilder();
        pList.forEach(p ->  builder.append(p.first.toString()).append(": ").append(p.second)
                                   .append(System.getProperty("line.separator")));
        return builder.toString();
    }

    public RowSnapshotSet.SetTableFilterDescription heavyFilter(final Schema schema) {
        RowSnapshotSet rss = new RowSnapshotSet(schema, this.hMap.keySet());
        return new RowSnapshotSet.SetTableFilterDescription(rss);
    }
}
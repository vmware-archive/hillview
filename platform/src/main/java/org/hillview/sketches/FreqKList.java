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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.RowSnapshotSet;
import org.hillview.utils.Converters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A data structure to store the K heavy hitters our of N elements, computed by the Misra-Gries
 * algorithm. We use the mergeable version of MG, as described in the ACM TODS paper "Mergeable
 * Summaries" by Agarwal et al., which gives a good error bound.
 * It stores a hash-map which contains the elements and their counts, along with counts
 * of the size of the input and the number of counters (K).
 */
public class FreqKList implements Serializable {
    /**
     * The size of the input table.
     */
    public long totalRows;
    /**
     * In MG it is the number of counters we store: the K in top-K heavy hitters.
     * In sampleHeavyHitters, it is used to store the number of samples taken
     */
    public int maxSize;
    /**
     * The number of times each row in the above table occurs in the original DataSet
     * (can be approximate depending on the context).
     */
    public final Object2IntOpenHashMap<RowSnapshot> hMap;

    public final double epsilon;


    public FreqKList(long totalRows, double epsilon, int maxSize,
                     Object2IntOpenHashMap<RowSnapshot> hMap) {
            this.totalRows = totalRows;
            this.epsilon = epsilon;
            this.maxSize = maxSize;
            this.hMap = hMap;
    }

    public FreqKList(long totalRows, double epsilon, Object2IntOpenHashMap<RowSnapshot> hMap) {
        this.totalRows = totalRows;
        this.epsilon = epsilon;
        this.hMap = hMap;
        this.maxSize = (int) Math.ceil(1/epsilon);
    }

    public FreqKList(List<RowSnapshot> rssList, double epsilon) {
        this.totalRows = 0;
        this.hMap = new Object2IntOpenHashMap<RowSnapshot>();
        rssList.forEach(rss -> this.hMap.put(rss, 0));
        this.epsilon = epsilon;
        this.maxSize= (int) Math.ceil(1/epsilon);
    }

    /**
     * Used to add two Lists that have counts for the same set of RowSnapShots. Behavior is not
     * determined if it is called with two lists that have different sets of keys. Meant to be used
     * by ExactFreqSketch.
     * @param that The list to be added to the current one.
     * @return Updated counts (existing counts are overwritten).
     */
    public FreqKList add(FreqKList that) {
        this.totalRows += that.totalRows;

        for (Object2IntMap.Entry<RowSnapshot> entry : this.hMap.object2IntEntrySet()) {
            int newVal = entry.getIntValue() + that.hMap.getOrDefault(entry.getKey(), 0);
            entry.setValue(newVal);
        }
        return this;
    }

    /**
     * @return Total distinct rows that are heavy hitters.
     */
    public int getDistinctRowCount() { return this.hMap.size(); }

    /**
     * This method returns the sum of counts computed by the data structure. This is always less
     * than totalRows, the number of rows in the table.
     * @return The sum of all counts stored in the hash-map.
     */
    private int getTotalCount() {
        return this.hMap.values().stream().reduce(0, Integer::sum);
    }

    /**
     * The error bound guaranteed by the "Mergeable Summaries" paper. It holds if a particular
     * sketch algorithm is applied to the Misra-Gries map. In particular, if the sum of observed
     * frequencies equals the total length of the table, the error is zero. Two notable properties
     * of this error bound:
     * - The frequency f(i) in the table is always an underestimate
     * - The true frequency lies between f(i) and f(i) + e, where e is the bound returned below.
     * @return Integer e such that if an element i has a count f(i) in the data
     * structure, then its true frequency in the range [f(i), f(i) +e].
     */
   public double getErrBound() {
        return  (this.totalRows - this.getTotalCount())/(this.maxSize + 1.0);
    }

    /**
     * @return The list of candidate heavy hitters. Used after running the Misra-Gries algorithm
     * (FreqKSketch) to figure out candidates for the top K.
     */
    public List<RowSnapshot> getList() {
        return new ArrayList<RowSnapshot>(this.hMap.keySet());
    }


    /**
     * Prunes the hashmap to retain only those RowSnapshots that occur with frequency above
     * 1/maxSize, and their frequencies.
     */
    public void filter(double threshold) {
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it = this.hMap.object2IntEntrySet().fastIterator();
             it.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> entry = it.next();
            if (entry.getIntValue() < (threshold))
                it.remove();
        }
    }

    public void filter(boolean isMG) {
        double threshold = this.epsilon * this.totalRows;
        if (isMG)
            threshold -= this.getErrBound();
        filter(threshold);
    }

    public void rescale() {
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it = this.hMap.object2IntEntrySet().fastIterator();
             it.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> entry = it.next();
            if (entry.getIntValue() < (this.epsilon * this.maxSize /1.1))
                it.remove();
            else
                this.hMap.put(entry.getKey(), (int) Math.ceil(entry
                        .getIntValue() * this.totalRows / this.maxSize));
        }
    }

    public Pair<List<RowSnapshot>, List<Integer>> getTop() {
        return getTop(this.hMap.size());
    }

    public Pair<List<RowSnapshot>, List<Integer>> getTop(int size) {
        List<Pair<RowSnapshot, Integer>> pList = new
                ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
        this.hMap.forEach((rs, j) -> pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        pList.sort((p1, p2) -> Integer.compare(
                Converters.checkNull(p2.second),
                Converters.checkNull(p1.second)));
        int minSize = Math.min(size, pList.size());
        List<RowSnapshot> listRows = new ArrayList<RowSnapshot>(minSize);
        List<Integer> listCounts = new ArrayList<Integer>(minSize);
        for (int i = 0; i < minSize; i++ ) {
            listRows.add(pList.get(i).first);
            listCounts.add(pList.get(i).second);
        }
        return new Pair<List<RowSnapshot>, List<Integer>>(listRows, listCounts);
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
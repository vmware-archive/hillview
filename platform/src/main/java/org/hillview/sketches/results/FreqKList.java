/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches.results;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.*;
import org.hillview.dataset.api.Pair;
import org.hillview.table.Schema;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.RowSnapshotSet;
import org.hillview.utils.Converters;
import org.hillview.utils.MutableInteger;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A abstract data structure to store the K heavy hitters out of N elements.
 * It stores a hash-map which contains the elements and their counts, along with counts
 * of the size of the input and the desired accuracy parameter epsilon.
 */
public class FreqKList implements Serializable {
    static final long serialVersionUID = 1;
    
    /**
     * The size of the input table.
     */
    public final long totalRows;
    /**
     * The number of times each row in the above table occurs in the original DataSet
     * (can be approximate depending on the context).
     */
    public final Object2IntOpenHashMap<RowSnapshot> hMap;

    /**
     * Stores a filtered and sorted version of the hashmap. Created during post-processing.
     */
    final List<Pair<RowSnapshot, Integer>> pList;

    public final double epsilon;

    /**
     * The maximum number of rows that we would like to display. We return only these many entries
     * from the sorted list of Heavy Hitters.
     */
    private static final int maxDisplay = 200;

    public FreqKList(long totalRows, double epsilon, Object2IntOpenHashMap<RowSnapshot> hMap) {
        this.totalRows = totalRows;
        this.epsilon = epsilon;
        this.hMap = hMap;
        this.pList = new ArrayList<Pair<RowSnapshot, Integer>>(this.hMap.size());
    }

    public int getSize() { return this.hMap.size(); }

    /**
     * @return The list of candidate heavy hitters. Used after running an approximate algorithm
     * to get candidates for computing exact counts.
     */
    public List<RowSnapshot> getList() {
        return new ArrayList<RowSnapshot>(this.hMap.keySet());
    }

    /**
     * The post-processing method that is used to extract the output of a Heavy Hitters sketch as a
     * NextKList (SmallTable + Counts). Each implementation does its own post-processing.
     * @param schema The schema of the RowSnapShots so that we can form a SmallTable.
     * @return A NextKList, which contains the top rows in sorted order of counts.
     */
    public NextKList getTop(Schema schema){
        return this.sortTopK(schema);
    }

    public void sortList() {
        this.hMap.forEach((rs, j) -> this.pList.add(new Pair<RowSnapshot, Integer>(rs, j)));
        this.pList.sort((p1, p2) -> Integer.compare(Converters.checkNull(p2.second),
                Converters.checkNull(p1.second)));
    }

    /**
     * Helper method that takes a List of <RowSnapShot, Integer> pairs, sorts them and puts them
     * into a NextKList. This is used by all Heavy Hitters sketches.
     * @param schema: The schema for the RowSnapShots.
     */
    public NextKList sortTopK(Schema schema) {
        this.pList.sort((p1, p2) -> Integer.compare(Converters.checkNull(p2.second),
                Converters.checkNull(p1.second)));
        int maxSize = Math.min(pList.size(), FreqKList.maxDisplay);
        List<RowSnapshot> listRows = new ArrayList<RowSnapshot>(maxSize);
        IntList listCounts = new IntArrayList(maxSize);
        for (int i = 0; i < maxSize; i++) {
            listRows.add(pList.get(i).first);
            listCounts.add(Utilities.toInt(Converters.checkNull(pList.get(i).second)));
        }
        return new NextKList(listRows, listCounts, schema, this.totalRows);
    }

    /**
     * Filters the list to retain only those RowSnapshots that occur with frequency above
     * a specified threshold.
     */
    void fkFilter(double threshold) {
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it =
             this.hMap.object2IntEntrySet().fastIterator(); it.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> entry = it.next();
            if (entry.getIntValue() < threshold) it.remove();
        }
    }

    /**
     * Method used to filter the table and keep/discard only those rows that match one of the heavy
     * hitters.
     * @param schema tells us which columns were used to compare with the heavy hitters.
     * @param includeSet specifies whether the matching rows are kept or discarded.
     * @return A table filter for only those rows that match one of the heavy hitters in Schema.
     */
    public RowSnapshotSet.SetTableFilterDescription getFilter(final Schema schema,
                                                              boolean includeSet) {
        RowSnapshotSet rss = new RowSnapshotSet(schema, this.hMap.keySet());
        return new RowSnapshotSet.SetTableFilterDescription(rss, includeSet);
    }

    public RowSnapshotSet.SetTableFilterDescription getFilter(Schema schema, boolean includeSet,
                                                              int[] rowIndices) {
        List<RowSnapshot> rsList = new ArrayList<RowSnapshot>();
        for (int rowIndex : rowIndices)
            rsList.add(this.pList.get(rowIndex).first);
        RowSnapshotSet rss = new RowSnapshotSet(schema, rsList);
        return new RowSnapshotSet.SetTableFilterDescription(rss, includeSet);
    }


    /**
     * This is a helper method that takes two FreqKLists (left and right) and returns a list
     * the union of their entries. If an element occurs in both, the frequencies add. This is used
     * for both the Misra-Gries sketch and the sampling sketch. The hashmap is post-processed
     * differently by each of them.
     * (FreqKListExact implements its own addition and does not use this.)
     */
    public static List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>>
    addLists(FreqKList left, FreqKList right) {
        Object2ObjectOpenHashMap<RowSnapshot, MutableInteger> resultMap =
                new Object2ObjectOpenHashMap<RowSnapshot, MutableInteger>(left.hMap.size() + right.hMap.size());
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it1 = left.hMap.object2IntEntrySet().
                fastIterator(); it1.hasNext(); ) {
            final Object2IntMap.Entry<RowSnapshot> it = it1.next();
            resultMap.put(it.getKey(), new MutableInteger(it.getIntValue()));
        }
        // Add values of right.hMap to resultMap
        for (ObjectIterator<Object2IntMap.Entry<RowSnapshot>> it1 = right.hMap.object2IntEntrySet().
                fastIterator(); it1.hasNext(); ) {
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

    public static Object2IntOpenHashMap<RowSnapshot> getUnion(
            @Nullable FreqKList left, @Nullable FreqKList right) {
        List<Object2ObjectMap.Entry<RowSnapshot, MutableInteger>> pList =
                FreqKList.addLists(Converters.checkNull(left), Converters.checkNull(right));
        Object2IntOpenHashMap<RowSnapshot> hm = new Object2IntOpenHashMap<RowSnapshot>(pList.size());
        for (Object2ObjectMap.Entry<RowSnapshot, MutableInteger> aPList : pList)
            hm.put(aPList.getKey(), aPList.getValue().get());
        return hm;
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
}

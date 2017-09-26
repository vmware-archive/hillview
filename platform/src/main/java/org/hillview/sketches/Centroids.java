/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.sketches;

import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

/**
 * This class represents a set of centroids of a partitioning of a table. The number of centroids
 * should not be too large, as they are shipped over the network. Since row/column entries are
 * only counted if the row is present in the column, it essentially computes the mean of each
 * column over only the non-missing entries.
 */
public class Centroids<T> implements Serializable {
    /**
     * Map from the partition key to the sum of the non-missing values in the partition in every
     * column. Every value is an array of doubles, where the i'th element is the sum of the
     * non-missing values encountered in the i'th column, for rows partitioned to the partition
     * specified by the key.
     */
    public final HashMap<T, double[]> sums;
    /**
     * Map from the partition key to the count of values in every column of that partition.
     * Every value is an array of longs, where the i'th element is the number of non-missing values
     * encountered in the i'th column, for rows partitioned to the partition specified by the key.
     * As we divide by the count to compute the final centroid, this means that the missing
     * values do not contribute to the centroids.
     */
    public final HashMap<T, long[]> counts;

    /**
     * Constructs a zero-centroids.
     */
    public Centroids() {
        this.sums = new HashMap<T, double[]>();
        this.counts = new HashMap<T, long[]>();
    }

    /**
     * Construct a centroids object that has processed the sums of the partitions in 'table'.
     * The final centroids are not computed yet, this should be done after the sketch finishes.
     * @param members MembershipSet that knows which rows to iterate over.
     * @param keyFunc Function that can determine the partition key of a row entry.
     * @param columns Column that define the nD space.
     */
    public Centroids(IMembershipSet members, Function<Integer, T> keyFunc, List<IColumn> columns) {
        this();
        int colIndex = 0;
        for (IColumn column : columns) {
            IRowIterator rowIterator = members.getIterator();
            int row = rowIterator.getNextRow();
            while (row >= 0) {
                // Don't add to the sum or count if the value is missing.
                if (column.isMissing(row))
                    continue;
                T key = keyFunc.apply(row);
                // Get the arrays for this partition from the HashMap (or create them if absent).
                double[] sums = this.sums.computeIfAbsent(key, k -> new double[columns.size()]);
                long[] counts = this.counts.computeIfAbsent(key, k -> new long[columns.size()]);
                // Update the sum and count of this partition in this column
                sums[colIndex] += column.getDouble(row);
                counts[colIndex]++;

                row = rowIterator.getNextRow();
            }
            colIndex++;
        }
    }

    /**
     * Compute the aggregate of two centroid objects. This simply sums the sums and counts.
     * @param other Object that holds information for the other set of centroids.
     * @return Centroid object that represents the aggregate of the rows that both objects represent.
     */
    public Centroids<T> union(Centroids<T> other) {
        // Make new centroid object, and copy the data from 'this'
        Centroids<T> result = new Centroids<T>();
        result.sums.putAll(this.sums);
        result.counts.putAll(this.counts);

        // Add the other's information into the result.
        other.sums.keySet().forEach((key) -> {
            if (result.sums.containsKey(key)) {
                // If the 'result' centroid already has the key, we have to sum the sums and counts for every column.
                double[] sum = other.sums.get(key);
                long[] count = other.counts.get(key); // other.counts must also contain the key (they share key sets, no need to check or loop twice).
                for (int colIndex = 0; colIndex < sum.length; colIndex++) {
                    result.sums.get(key)[colIndex] += sum[colIndex];
                    result.counts.get(key)[colIndex] += count[colIndex]; // Same assumption can be made here.
                }
            } else {
                // No need to iterate/sum. Simply use the other's array.
                result.sums.put(key, other.sums.get(key));
                result.counts.put(key, other.counts.get(key));
            }
        });

        return result;
    }

    /**
     * Computes the centroids, based on the information in the 'sums' and 'counts' fields.
     * This only has to be done once: when the sketch has finished.
     * @return HashMap with the computed centroid for every partition.
     */
    public HashMap<T, double[]> computeCentroids() {
        HashMap<T, double[]> centroids = new HashMap<T, double[]>();
        this.sums.forEach((key, sum) -> {
            long[] count = this.counts.get(key);
            centroids.put(key, new double[sum.length]);
            for (int colIndex = 0; colIndex < sum.length; colIndex++) {
                if (count[colIndex] > 0)
                    centroids.get(key)[colIndex] = sum[colIndex] / count[colIndex];
                else
                    centroids.get(key)[colIndex] = java.lang.Float.NaN;
            }
        });
        return centroids;
    }
}

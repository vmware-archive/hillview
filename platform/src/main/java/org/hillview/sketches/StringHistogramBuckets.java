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

import org.hillview.table.api.IColumn;

import java.util.Arrays;

/**
 * Left endpoints for string buckets.
 */
public class StringHistogramBuckets implements IHistogramBuckets {
    private final String minValue;
    private final String lastBoundary;
    private final int numOfBuckets;
    /**
     * These are the *left endpoints* of the buckets.
     */
    public final String[] leftBoundaries;

    public StringHistogramBuckets(final String[] leftBoundaries) {
        if (leftBoundaries.length == 0)
            throw new IllegalArgumentException("Boundaries of buckets can't be empty");
        if (!isSorted(leftBoundaries))
            throw new IllegalArgumentException("Boundaries of buckets have to be sorted");
        this.leftBoundaries = leftBoundaries;
        this.numOfBuckets = leftBoundaries.length;
        this.minValue = this.leftBoundaries[0];
        this.lastBoundary = this.leftBoundaries[this.leftBoundaries.length - 1];
    }

    /**
     * Checks that an array is strongly sorted.
     */
    private static boolean isSorted(final String[] a) {
        for (int i = 0; i < (a.length - 1); i++)
            if (a[i].compareTo(a[i + 1]) >= 0)
                return false;
        return true;
    }

    public int indexOf(String item) {
        if ((item.compareTo(this.minValue)) < 0)
            return -1;
        if (item.compareTo(this.lastBoundary) >= 0)
            // Anything bigger than the lastBoundary is in the last bucket
            return this.numOfBuckets - 1;
        int index = Arrays.binarySearch(leftBoundaries, item);
        // This method returns index of the search key, if it is contained in the array,
        // else it returns (-(insertion point) - 1). The insertion point is the point
        // at which the key would be inserted into the array: the index of the first
        // element greater than the key, or a.length if all elements in the array
        // are less than the specified key.
        if (index < 0) {
            index = -index - 1;
            if (index == 0)
                // before first element
                return -1;
            return index - 1;
        }
        return index;
    }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        String item = column.getString(rowIndex);
        if (item == null)
            // This should not really happen.
            return -1;
        return this.indexOf(item);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }
}

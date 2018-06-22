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
 * Bucket boundaries for string histograms.
 */
public class StringBucketsDescription implements IHistogramBuckets {
    private final String minValue;
    private final String maxValue;
    private final int numOfBuckets;
    private final String[] boundaries;

    /**
     * The assumption is that the all buckets are only left inclusive except the right one which
     * is right inclusive. Boundaries has to be strongly sorted.
     */
    public StringBucketsDescription(final String[] boundaries) {
        if (boundaries.length == 0)
            throw new IllegalArgumentException("Boundaries of buckets can't be empty");
        if (!isSorted(boundaries))
            throw new IllegalArgumentException("Boundaries of buckets have to be sorted");
        this.boundaries = boundaries;
        this.numOfBuckets = boundaries.length - 1;
        this.minValue = this.boundaries[0];
        this.maxValue = this.boundaries[this.numOfBuckets];
    }

    /**
     * Checks that an array is strongly sorted
     */
    private static boolean isSorted(final String[] a) {
        for (int i = 0; i < (a.length - 1); i++)
            if (a[i].compareTo(a[i + 1]) >= 0)
                return false;
        return true;
    }

    public int indexOf(String item) {
        if ((item.compareTo(this.minValue)) < 0 || (item.compareTo(this.maxValue)) > 0)
            return -1;
        if (item.equals(this.maxValue))
            return this.numOfBuckets;
        int index = Arrays.binarySearch(boundaries, item);
        // This method returns index of the search key, if it is contained in the array,
        // else it returns (-(insertion point) - 1). The insertion point is the point
        // at which the key would be inserted into the array: the index of the first
        // element greater than the key, or a.length if all elements in the array
        // are less than the specified key.
        if (index < 0) {
            index = -index - 1;
            if (index == 0)
                // before first element
                index = -1;
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

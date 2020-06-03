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

import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * A set of histogram buckets explicitly specified.
 */
public abstract class ExplicitHistogramBuckets<T extends Comparable<T>>
        implements IHistogramBuckets {
    static final long serialVersionUID = 1;

    public final String column;
    public final T minValue;
    private final T lastBoundary;
    private final int bucketCount;
    @Nullable
    public final T maxValue;
    /**
     * These are the *left endpoints* of the buckets.
     */
    public final T[] leftBoundaries;

    ExplicitHistogramBuckets(String column, final T[] leftBoundaries) {
        // We don't make this protected so users can instantiate it explicitly
        if (leftBoundaries.length == 0)
            throw new IllegalArgumentException("Boundaries of buckets can't be empty");
        Utilities.checkSorted(leftBoundaries);
        this.column = column;
        this.leftBoundaries = leftBoundaries;
        this.bucketCount = leftBoundaries.length;
        this.minValue = this.leftBoundaries[0];
        this.maxValue = null;
        this.lastBoundary = this.leftBoundaries[this.leftBoundaries.length - 1];
    }

    ExplicitHistogramBuckets(String column, T[] leftBoundaries, T maxValue) {
        if (leftBoundaries.length == 0)
            throw new IllegalArgumentException("Boundaries of buckets can't be empty");
        Utilities.checkSorted(leftBoundaries);
        this.column = column;
        this.leftBoundaries = leftBoundaries;
        this.bucketCount = leftBoundaries.length;
        this.minValue = this.leftBoundaries[0];
        this.maxValue = maxValue;
        this.lastBoundary = this.leftBoundaries[this.leftBoundaries.length - 1];
    }

    @Override
    public String getColumn() {
        return this.column;
    }

    public int indexOf(T item) {
        if ((item.compareTo(this.minValue)) < 0)
            return -1;
        if (this.maxValue != null && item.compareTo(this.maxValue) >= 0)
            return -1;
        if (item.compareTo(this.lastBoundary) >= 0)
            // Anything bigger than the lastBoundary is in the last bucket
            return this.bucketCount - 1;
        int index = Arrays.binarySearch(this.leftBoundaries, item);
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
    public int getBucketCount() { return this.bucketCount; }

    public T leftMargin(int bucketIndex) {
        return this.leftBoundaries[bucketIndex];
    }
}

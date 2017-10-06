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

/**
 * Metadata for one dimensional buckets held by a histogram
 */
public class BucketsDescription implements IBucketsDescription {
    private final double minValue;
    private final double maxValue;
    private final int numOfBuckets;
    private final double[] boundaries;

    /**
     * The assumption is that the all buckets are only left inclusive except the right one which
     * is right inclusive. Boundaries has to be strongly sorted.
     */
    public BucketsDescription(final double[] boundaries) {
        if (boundaries.length == 0)
            throw new IllegalArgumentException("Boundaries of buckets can't be empty");
        if (!isSorted(boundaries))
            throw new IllegalArgumentException("Boundaries of buckets have to be sorted");
        this.boundaries = new double[boundaries.length];
        System.arraycopy(boundaries, 0, this.boundaries, 0, boundaries.length);
        this.numOfBuckets = boundaries.length - 1;
        this.minValue = this.boundaries[0];
        this.maxValue = this.boundaries[this.numOfBuckets];
    }

    /**
     * Checks that an array is strongly sorted
     */
    private static boolean isSorted(final double[] a) {
        for (int i = 0; i < (a.length - 1); i++)
            if (a[i] > a[i + 1])
                return false;
        return true;
    }

    @Override
    public int indexOf(final double item) {
        if ((item < this.minValue) || (item > this.maxValue))
            return -1;
        if (item == this.maxValue)
            return this.numOfBuckets - 1;
        int lo = 0;
        int hi = this.boundaries.length - 1;
        while (lo <= hi) {
            int mid = lo + ((hi - lo) / 2);
            if (item < this.boundaries[mid]) hi = mid ;
            else if (item >= this.boundaries[mid + 1]) lo = mid;
            else return mid;
        }
        throw new IllegalStateException("bug in the indexOf function");
    }

    @Override
    public double getLeftBoundary(int index) {
        if ((index < 0) || (index >= this.numOfBuckets))
            throw new IllegalArgumentException("Bucket index out of range");
        return this.boundaries[index];
    }

    @Override
    public double getRightBoundary(final int index) {
        if (index == (this.numOfBuckets - 1))
            return this.maxValue;
        return this.getLeftBoundary(index + 1);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }
}
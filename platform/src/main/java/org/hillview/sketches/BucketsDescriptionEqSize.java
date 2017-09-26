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
 *
 */

package org.hillview.sketches;

/**
 * MetaData for one dimensional buckets of equal size
 */
public class BucketsDescriptionEqSize implements IBucketsDescription {
    private final double minValue;
    private final double maxValue;
    private final int numOfBuckets;
    private final double range;

    public BucketsDescriptionEqSize(final double minValue, final double maxValue, final int numOfBuckets) {
        if (maxValue < minValue || numOfBuckets <= 0)
            throw new IllegalArgumentException("Negative range or number of buckets");
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.range = this.maxValue - this.minValue;
        if (this.range <= 0)
            this.numOfBuckets = 1;  // ignore specified number of buckets
        else
            this.numOfBuckets = numOfBuckets;
    }

    @Override
    public int indexOf(final double item) {
        if ((item < this.minValue) || (item > this.maxValue))
            return -1;
        if (item >= this.maxValue)
            return this.numOfBuckets - 1;
        return (int) ((this.numOfBuckets * (item - this.minValue)) / this.range);
    }

    @Override
    public double getLeftBoundary(final int index) {
        if ((index < 0) || (index >= this.numOfBuckets))
            throw new IllegalArgumentException("Bucket index out of range");
        return this.minValue + ((this.range * index) / this.numOfBuckets);
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

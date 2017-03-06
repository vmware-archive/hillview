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

package org.hiero.sketch.spreadsheet;

/**
 * MetaData for one dimensional buckets of equal size
 */
public class BucketsDescriptionEqSize implements IBucketsDescription1D {
    private final double minValue;
    private final double maxValue;
    private final int numOfBuckets;

    public BucketsDescriptionEqSize(final double minValue, final double maxValue, final int numOfBuckets) {
        if (maxValue <= minValue)
            throw new IllegalArgumentException("Buckets range cannot be empty");
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.numOfBuckets = numOfBuckets;
    }

    @Override
    public int indexOf(final double item) {
        if ((item < this.minValue) || (item > this.maxValue))
            return -1;
        if (item >= this.maxValue)
            return this.numOfBuckets - 1;
        return (int) (this.numOfBuckets * (item - this.minValue)) / (int) (this.maxValue - this.minValue);
    }

    @Override
    public double getLeftBoundary(final int index) {
        if ((index < 0) || (index >= this.numOfBuckets))
            throw new IllegalArgumentException("Bucket index out of range");
        return this.minValue + (((this.maxValue - this.minValue) * index) / this.numOfBuckets);
    }

    @Override
    public double getRightBoundary(final int index) {
        if (index == (this.numOfBuckets - 1))
            return this.maxValue;
        return this.getLeftBoundary(index + 1);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        BucketsDescriptionEqSize that = (BucketsDescriptionEqSize) o;

        if (Double.compare(that.minValue, this.minValue) != 0) return false;
        if (Double.compare(that.maxValue, this.maxValue) != 0) return false;
        return this.numOfBuckets == that.numOfBuckets;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(this.minValue);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(this.maxValue);
        result = (31 * result) + (int) (temp ^ (temp >>> 32));
        result = (31 * result) + this.numOfBuckets;
        return result;
    }
}

/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

/**
 * Buckets for computing a histogram of data that can be converted to a double.
 * The last bucket is right-inclusive.
 */
public class DoubleHistogramBuckets implements IHistogramBuckets {
    private final double minValue;
    private final double maxValue;
    private final int numOfBuckets;
    private final double range;

    public DoubleHistogramBuckets(final double minValue, final double maxValue, final int numOfBuckets) {
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

    public int indexOf(double value) {
        if ((value < this.minValue) || (value > this.maxValue))
            return -1;
        // As overflow can occur when 'item' is very close to 'this.maxValue', clamp the resulting index.
        return Math.min((int) ((this.numOfBuckets * (value - this.minValue)) / this.range), this.numOfBuckets - 1);
    }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        double item = column.asDouble(rowIndex);
        return this.indexOf(item);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }
}

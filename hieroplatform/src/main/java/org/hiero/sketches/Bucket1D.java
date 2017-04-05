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

package org.hiero.sketches;

import org.hiero.dataset.api.IJson;

import javax.annotation.Nullable;

/**
 * A one dimensional bucket designed for a Histogram1D
 */
public class Bucket1D implements IJson {
    @Nullable
    private Object minObject;
    @Nullable
    private Object maxObject;
    private double minValue;
    private double maxValue;
    private long count;

    public Bucket1D() {
        this.minObject = null;
        this.maxObject = null;
        this.minValue = Double.MAX_VALUE;
        this.maxValue = 0;
        this.count = 0;
    }

    private Bucket1D(final long count, final double minValue, final double maxValue,
                     @Nullable final Object minObject,
                     @Nullable final Object maxObject) {
        this.count = count;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.minObject = minObject;
        this.maxObject = maxObject;
    }

    @Nullable
    public Object getMinObject() { return this.minObject; }

    @Nullable
    public Object getMaxObject() { return this.maxObject; }

    public double getMinValue() { return this.minValue; }

    public double getMaxValue() { return this.maxValue; }

    public long getCount() { return this.count; }

    public void add(final double item, @Nullable final Object currObject) {
        if (this.count == 0) {
            this.minValue = item;
            this.minObject = currObject;
            this.maxValue = item;
            this.maxObject = currObject;
        } else if (item < this.minValue) {
            this.minValue = item;
            this.minObject = currObject;
        } else if (item > this.maxValue) {
            this.maxValue = item;
            this.maxObject = currObject;
        }
        this.count++;
    }

    public boolean isEmpty() { return this.count == 0; }

    /**
     * @return A bucket with the union count of the two buckets and the min/max updated accordingly. Procedure allows
     * both buckets to have objects of different types.
     */
    public Bucket1D union(final Bucket1D otherBucket) {
        long ucount = this.count + otherBucket.count;
        double uMinValue, uMaxValue;
        Object uMinObject, uMaxObject;
        if (this.minValue < otherBucket.minValue) {
            uMinValue = this.minValue;
            uMinObject = this.minObject;
        } else {
            uMinValue = otherBucket.minValue;
            uMinObject = otherBucket.minObject;
        }
        if (this.maxValue > otherBucket.maxValue) {
            uMaxValue = this.maxValue;
            uMaxObject = this.maxObject;
        } else {
            uMaxValue = otherBucket.maxValue;
            uMaxObject = otherBucket.maxObject;
        }
        return new Bucket1D(ucount, uMinValue, uMaxValue, uMinObject, uMaxObject);
    }
}
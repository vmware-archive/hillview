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

package org.hillview.table.columns;

/**
 * This class represents metadata used for computing differentially-private mechanisms
 * for columns storing doubles.
 */
public class DoubleColumnPrivacyMetadata extends ColumnPrivacyMetadata {
    /**
     * Minimum quantization interval: users will only be able to
     * query ranges that are a multiple of this size.
     * This field is particularly useful for implementing the dyadic interval tree
     * in the binary mechanism of Chan, Song, Shi '11 (https://eprint.iacr.org/2010/076.pdf).
     */
    public final double granularity;
    /**
     * Fixed global minimum value for the column. Should be computable from
     * public information or otherwise uncorrelated with the data.
     */
    public final double globalMin;
    /**
     * Fixed global maximum value. Should be computable from
     * public information or otherwise uncorrelated with the data.
     */
    public final double globalMax;

    /**
     * Create a privacy metadata for a numeric-type column.
     * @param epsilon      Differential privacy parameter.
     * @param granularity  Size of a bucket for quantized data.
     * @param globalMin    Minimum value expected in the column.
     * @param globalMax    Maximum value expected in column.
     */
    public DoubleColumnPrivacyMetadata(double epsilon, double granularity,
                                       double globalMin, double globalMax) {
        super(epsilon);
        this.granularity = granularity;
        this.globalMin = globalMin;
        this.globalMax = globalMax;
        if (globalMin > globalMax)
            throw new IllegalArgumentException("Negative data range: " + globalMin + " - " + globalMax);
        if (this.granularity <= 0)
            throw new IllegalArgumentException("Granularity must be positive: " + this.granularity);
        double intervals = (this.globalMax - this.globalMin) / this.granularity;
        if (Math.abs(intervals - Math.round(intervals)) > .001)
            throw new IllegalArgumentException("Granularity does not divide range into an integer number of intervals");
    }

    public double roundDown(double value) {
        if (value <= this.globalMin)
            throw new RuntimeException("Value smaller than min: " + value + "<" + this.globalMin);
        if (value >= this.globalMax)
            return this.globalMax;
        double intervals = Math.floor((value - this.globalMin) / this.granularity);
        return this.globalMin + intervals * this.granularity;
    }

    public double roundUp(double value) {
        if (value <= this.globalMin)
            return this.globalMin;
        if (value >= this.globalMax)
            throw new RuntimeException("Value greater than max: " + value + ">" + this.globalMax);
        double intervals = Math.floor((value - this.globalMin) / this.granularity);
        return this.globalMin + (intervals + 1) * this.granularity;
    }
}

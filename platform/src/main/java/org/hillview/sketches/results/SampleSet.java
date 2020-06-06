/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.sketches.results;

import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.dataset.api.IScalable;
import org.hillview.sketches.ReservoirSampleWorkspace;
import org.hillview.utils.Randomness;
import org.hillview.utils.Utilities;

import java.util.Arrays;

/**
 * A set of samples from a numeric distribution extracted using
 * reservoir sampling.  Also maintains the min and the max.
 */
public class SampleSet implements IJsonSketchResult, IScalable<SampleSet> {
    static final long serialVersionUID = 1;

    /**
     * Minimum value in distribution.
     */
    public double min;
    /**
     * Maximum value in distribution.
     */
    public double max;
    /**
     * A list of samples computed by sampling with the
     * specified rate.
     */
    public double[] samples;
    /**
     * Number of missing values.
     */
    public long missing;
    /**
     * Number of non-null elements scanned.
     */
    public long count;
    public final long seed;

    public SampleSet(int sampleCount, long seed) {
        this(sampleCount, 0, seed);
    }

    private SampleSet(int sampleCount, long count, long seed) {
        this.missing = 0;
        this.samples = new double[sampleCount];
        this.count = count;
        this.seed = seed;
    }

    public int size() {
        return this.samples.length;
    }

    public boolean empty() {
        return this.count == 0;
    }

    /**
     * Combine two sets of numeric samples.
     */
    public SampleSet add(SampleSet other) {
        if (this.samples.length != other.samples.length)
            throw new RuntimeException("Merging incompatible sample sets");
        SampleSet result = new SampleSet(this.samples.length, this.count + other.count);
        if (this.empty()) {
            result.min = other.min;
            result.max = other.max;
        } else if (other.empty()) {
            result.min = this.min;
            result.max = this.max;
        } else {
            result.min = Math.min(this.min, other.min);
            result.max = Math.max(this.max, other.max);
        }
        result.missing = this.missing + other.missing;
        result.count = this.count + other.count;
        Randomness random = new Randomness(this.seed);
        if (this.count < this.samples.length) {
            copyAndMerge(random, result.samples, this.samples, this.count, other.samples, other.count);
        } else if (other.count < this.samples.length) {
            copyAndMerge(random, result.samples, other.samples, other.count, this.samples, this.count);
        } else {
            // both full
            double p = (double)this.count / result.count;
            for (int i = 0; i < result.samples.length; i++) {
                double d = random.nextDouble();
                double r;
                if (d < p)
                    r = this.samples[i];
                else
                    r = other.samples[i];
                result.samples[i] = r;
            }
        }
        return result;
    }

    @Override
    public SampleSet rescale(double samplingRate) {
        // Nothing to do
        return this;
    }

    private static void copyAndMerge(Randomness random, double[] dest, double[] smallArray, long smallCount,
                                     double[] bigArray, long bigCount) {
        int filled = (int)Math.min(dest.length, bigCount);
        // Fill result first from big array
        System.arraycopy(bigArray, 0, dest, 0, filled);
        int i = 0;
        // Fill the remaining space from the small array
        for (; i < Math.min(dest.length - filled, (int)smallCount); i++)
            dest[i + filled] = smallArray[i];
        // Finally, start sampling from the remaining small array
        for (; i < (int)smallCount; i++) {
            double d = smallArray[i];
            int index = random.nextInt(i + filled);
            if (index < dest.length)
                dest[index] = d;
        }
    }

    /**
     * Here is a number from the distribution; sample it.
     */
    public void add(ReservoirSampleWorkspace workspace, double d) {
        if (this.empty()) {
            this.min = d;
            this.max = d;
        } else {
            this.min = Math.min(this.min, d);
            this.max = Math.max(this.max, d);
        }
        this.count++;
        int index = workspace.sampleIndex();
        if (index >= 0)
            this.samples[index] = d;
    }

    /**
     * Extract only the specified number of quantiles from the NumericSamples.
     * @param expectedCount Number of empirical quantiles to extract.
     */
    public SampleSet quantiles(int expectedCount) {
        SampleSet result;
        if (this.count < expectedCount) {
            result = new SampleSet((int)this.count, this.count);
            System.arraycopy(this.samples, 0, result.samples, 0, result.samples.length);
            Arrays.sort(result.samples);
        } else {
            double[] valid;
            if (this.count < this.samples.length) {
                // Not all values in array are valid; only sort the valid ones
                valid = new double[(int)this.count];
                System.arraycopy(this.samples, 0, valid, 0, (int)this.count);
            } else {
                // the entire array this.samples has valid data
                valid = this.samples;
            }
            Arrays.sort(valid);
            double[] small = Utilities.decimate(valid, Math.floorDiv(valid.length, expectedCount));
            result = new SampleSet(small.length - 1, this.count);
            System.arraycopy(small, 1, result.samples, 0, result.samples.length);  // skip 0-th quantile
        }
        result.min = this.min;
        result.max = this.max;
        result.count = this.count;
        result.missing = this.missing;
        // Sanity check
        for (double v: result.samples)
            if (!(v >= result.min && v <= result.max))
                throw new RuntimeException(
                        "Illegal sample in set " + v + " count " + this.count + " expected " + expectedCount);
        return result;
    }

    public void addMissing() {
        this.missing++;
    }
}

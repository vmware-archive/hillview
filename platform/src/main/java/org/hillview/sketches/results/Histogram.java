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

import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;

/**
 * One dimensional histogram.
 */
public class Histogram implements IJsonSketchResult, IHistogram {
    static final long serialVersionUID = 1;
    
    public long[] buckets;
    /**
     * Count of missing (NULL) values.
     */
    protected long missingCount;
    /**
     * Confidence intervals of the buckets.  May be null.
     */
    @Nullable
    public int[] confidence;
    /**
     * Confidence interval of missingData.
     */
    public int  missingConfidence;

    public Histogram(int buckets, boolean allocateConfidence) {
        this.buckets = new long[buckets];
        if (allocateConfidence)
            this.confidence = new int[buckets];
    }

    public Histogram(int buckets) {
        this(buckets, false);
    }

    public Histogram(long[] data, long missing) {
        this.buckets = data;
        this.missingCount = missing;
    }

    public void rescale(double sampleRate) {
        if (sampleRate >= 1)
            return;
        this.missingCount = Utilities.toLong((double) this.missingCount / sampleRate);
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = Utilities.toLong((double) this.buckets[i] / sampleRate);
        }
    }

    public void add(IColumn column, int currRow, IHistogramBuckets buckets) {
        if (column.isMissing(currRow))
            this.missingCount++;
        else {
            int index = buckets.indexOf(column, currRow);
            if (index >= 0)
                this.buckets[index]++;
        }
    }

    public void create(final IColumn column, IMembershipSet membershipSet,
                       IHistogramBuckets buckets,
                       double sampleRate, long seed, boolean enforceRate) {
        if (sampleRate <= 0)
            throw new RuntimeException("Negative sampling rate");
        final ISampledRowIterator myIter = membershipSet.getIteratorOverSample(
                sampleRate, seed, enforceRate);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            this.add(column, currRow, buckets);
            currRow = myIter.getNextRow();
        }
        this.rescale(myIter.rate());
    }

    public long getMissingCount() { return this.missingCount; }

    /**
     * @return the index's bucket count
     */
    public long getCount(final int index) { return this.buckets[index]; }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram union(Histogram otherHistogram) {
        Histogram unionH = new Histogram(this.getBucketCount());
        for (int i = 0; i < unionH.getBucketCount(); i++) {
            unionH.buckets[i] = this.buckets[i] + otherHistogram.buckets[i];
        }
        unionH.missingCount = this.missingCount + otherHistogram.missingCount;
        return unionH;
    }

    public int getBucketCount() { return this.buckets.length; }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i=0; i < this.buckets.length; i++) {
            builder.append("[");
            builder.append(i);
            builder.append("]=");
            builder.append(this.buckets[i]);
            builder.append(" ");
        }
        return builder.toString();
    }

    /**
     * Returns a new histogram where each bucket is the prefix sum of the prior buckets.
     */
    public Histogram integrate() {
        Histogram result = new Histogram(this.getBucketCount());
        long previous = 0;
        for (int i = 0; i < this.buckets.length; i++) {
            long next = previous + this.buckets[i];
            result.buckets[i] = next;
            previous = next;
        }
        return result;
    }

    public boolean same(Histogram other) {
        if (this.missingCount != other.missingCount)
            return false;
        if (this.buckets.length != other.buckets.length)
            return false;
        for (int i = 0; i < this.buckets.length; i++)
            if (this.buckets[i] != other.buckets[i])
                return false;
        return true;
    }
}

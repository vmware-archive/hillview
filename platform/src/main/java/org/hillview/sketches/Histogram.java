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

import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * One dimensional histogram where buckets are just longs and not a full object.
 */
public class Histogram implements Serializable {
    private final long[] buckets;
    private long missingData;
    private long outOfRange;
    private final double rate;
    private final IBucketsDescription bucketDescription;

    public Histogram(final IBucketsDescription bucketDescription) {
        this.bucketDescription = bucketDescription;
        this.buckets = new long[bucketDescription.getNumOfBuckets()];
        this.rate = 1.0;
    }

    void addValue(final double val) {
        int index = this.bucketDescription.indexOf(val);
        if (index >= 0)
            this.buckets[index]++;
        else this.outOfRange++;
    }

    public void createHistogram(final IColumn column, IMembershipSet membershipSet,
                                @Nullable final IStringConverter converter) {
        if (this.rate < 1)
            membershipSet = membershipSet.sample(this.rate);
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (column.isMissing(currRow))
                this.missingData++;
            else {
                double val = column.asDouble(currRow, converter);
                int index = this.bucketDescription.indexOf(val);
                if (index >= 0)
                    this.buckets[index]++;
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's bucket count
     */
    public long getCount(final int index) { return this.buckets[index]; }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram union(Histogram otherHistogram) {
        if (!this.bucketDescription.equals(otherHistogram.bucketDescription))
            throw new IllegalArgumentException("Histogram union without matching buckets");
        Histogram unionH = new Histogram(this.bucketDescription);
        for (int i = 0; i < unionH.bucketDescription.getNumOfBuckets(); i++)
            unionH.buckets[i] = this.buckets[i] + otherHistogram.buckets[i];
        unionH.missingData = this.missingData + otherHistogram.missingData;
        unionH.outOfRange = this.outOfRange + otherHistogram.outOfRange;
        return unionH;
    }

    public Histogram createCDF() {
        Histogram cdf = new Histogram(this.bucketDescription);
        cdf.buckets[0] = this.buckets[0];
        for (int i = 1; i < this.bucketDescription.getNumOfBuckets(); i++)
            cdf.buckets[i] = cdf.buckets[i - 1] + this.buckets[i];
        return cdf;
    }

    public int getNumOfBuckets() { return this.bucketDescription.getNumOfBuckets(); }

    public void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                                      @Nullable final IStringConverter converter, double sampleRate) {
        this.createHistogram(column, membershipSet.sample(sampleRate), converter);
    }

    public void createSampleHistogram(final IColumn column, final IMembershipSet membershipSet,
                                      @Nullable final IStringConverter converter, double sampleRate, long seed) {
        this.createHistogram(column, membershipSet.sample(sampleRate, seed), converter);
    }
}

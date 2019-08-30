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
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ISampledRowIterator;

/**
 * One dimensional histogram.
 */
public class Histogram extends HistogramBase {
    private final IHistogramBuckets bucketDescription;

    public Histogram(final IHistogramBuckets bucketDescription) {
        this.bucketDescription = bucketDescription;
        this.buckets = new long[bucketDescription.getNumOfBuckets()];
    }

    public IHistogramBuckets getBucketDescription() {
        return this.bucketDescription;
    }

    public void rescale(double sampleRate) {
        if (sampleRate >= 1)
            return;
        this.missingData = (long) ((double) this.missingData / sampleRate);
        for (int i = 0; i < this.buckets.length; i++)
            this.buckets[i] = (long) ((double) this.buckets[i] / sampleRate);
    }

    public void add(IColumn column, int currRow) {
        if (column.isMissing(currRow))
            this.missingData++;
        else {
            int index = this.bucketDescription.indexOf(column, currRow);
            if (index >= 0)
                this.buckets[index]++;
        }
    }

    public void create(final IColumn column, IMembershipSet membershipSet,
                       double sampleRate, long seed, boolean enforceRate) {
        if (sampleRate <= 0)
            throw new RuntimeException("Negative sampling rate");
        final ISampledRowIterator myIter = membershipSet.getIteratorOverSample(
                sampleRate, seed, enforceRate);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            this.add(column, currRow);
            currRow = myIter.getNextRow();
        }
        this.rescale(myIter.rate());
    }

    public long getMissingData() { return this.missingData; }

    /**
     * @return the index's bucket count
     */
    public long getCount(final int index) { return this.buckets[index]; }

    /**
     * @param  otherHistogram with the same bucketDescription
     * @return a new Histogram which is the union of this and otherHistogram
     */
    public Histogram union(Histogram otherHistogram) {
        Histogram unionH = new Histogram(this.bucketDescription);
        for (int i = 0; i < unionH.bucketDescription.getNumOfBuckets(); i++)
            unionH.buckets[i] = this.buckets[i] + otherHistogram.buckets[i];
        unionH.missingData = this.missingData + otherHistogram.missingData;
        return unionH;
    }

    public int getNumOfBuckets() { return this.bucketDescription.getNumOfBuckets(); }

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
}

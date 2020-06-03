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
 * A 2-dimensional histogram.
 */
public class Heatmap implements IJsonSketchResult {
    static final long serialVersionUID = 1;
    
    public final long[][] buckets;
    @Nullable
    public int[][] confidence;
    private long missingData; // number of items missing on both columns
    private Histogram histogramMissingX; // dim1 is missing, dim2 exists
    private Histogram histogramMissingY; // dim2 is missing, dim1 exists
    private long totalSize;
    public final int xBucketCount;
    public final int yBucketCount;

    public Heatmap(final int xBucketCount,
                   final int yBucketCount,
                   boolean allocateConfidence) {
        this.xBucketCount = xBucketCount;
        this.yBucketCount = yBucketCount;
        this.buckets = new long[xBucketCount][yBucketCount];
        // Automatically initialized to 0
        this.histogramMissingX = new Histogram(yBucketCount, allocateConfidence);
        this.histogramMissingY = new Histogram(xBucketCount, allocateConfidence);
        if (allocateConfidence)
            this.confidence = new int[this.xBucketCount][this.yBucketCount];
    }

    public Heatmap(final int xBucketCount,
                   final int yBucketCount) {
        this(xBucketCount, yBucketCount, false);
    }

    public void createHeatmap(final IColumn columnD1, final IColumn columnD2,
                              IHistogramBuckets xBuckets, IHistogramBuckets yBuckets,
                              final IMembershipSet membershipSet, double samplingRate,
                              final long seed, final boolean enforceRate) {
        final ISampledRowIterator myIter = membershipSet.getIteratorOverSample(
                samplingRate, seed, enforceRate);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            boolean isMissingD1 = columnD1.isMissing(currRow);
            boolean isMissingD2 = columnD2.isMissing(currRow);
            if (isMissingD1 || isMissingD2) {
                if (!isMissingD1) {
                    // only column 2 is missing
                    this.histogramMissingY.add(columnD1, currRow, xBuckets);
                } else if (!isMissingD2) {
                    // only column 1 is missing
                    this.histogramMissingX.add(columnD2, currRow, yBuckets);
                } else {
                    // both are missing
                    this.missingData++;
                }
            } else {
                int index1 = xBuckets.indexOf(columnD1, currRow);
                int index2 = yBuckets.indexOf(columnD2, currRow);
                if ((index1 >= 0) && (index2 >= 0)) {
                    this.buckets[index1][index2]++;
                    this.totalSize++;
                }
            }
            currRow = myIter.getNextRow();
        }
        samplingRate = myIter.rate();
        if (samplingRate < 1) {
            this.histogramMissingX.rescale(myIter.rate());
            this.histogramMissingY.rescale(myIter.rate());
            this.missingData = Utilities.toLong(((double) this.missingData / samplingRate));
            for (int i = 0; i < this.buckets.length; i++)
                for (int j = 0; j < this.buckets[i].length; j++)
                    this.buckets[i][j] = Utilities.toLong(((double) this.buckets[i][j] / samplingRate));
        }
    }

    public Histogram getMissingHistogramD1() { return this.histogramMissingX; }

    public long getSize() { return this.totalSize; }

    public Histogram getMissingHistogramD2() { return this.histogramMissingY; }

    public long getMissingData() { return this.missingData; }

    /**
     * @return the index's count
     */
    public long getCount(final int index1, final int index2) { return this.buckets[index1][index2]; }

    /**
     * @param  other   A Heatmap with the same bucketDescriptions
     * @return a new Heatmap which is the union of this and other.
     * Note: this assumes the confidence is null.
     */
    public Heatmap union(Heatmap other) {
        Heatmap unionH = new Heatmap(this.xBucketCount, this.yBucketCount);
        for (int i = 0; i < xBucketCount; i++)
            for (int j = 0; j < this.yBucketCount; j++)
                unionH.buckets[i][j] = this.buckets[i][j] + other.buckets[i][j];
        unionH.missingData = this.missingData + other.missingData;
        unionH.totalSize = this.totalSize + other.totalSize;
        unionH.histogramMissingX = this.histogramMissingX.union(other.histogramMissingX);
        unionH.histogramMissingY = this.histogramMissingY.union(other.histogramMissingY);
        return unionH;
    }

    public boolean same(Heatmap other) {
        if (this.buckets.length != other.buckets.length)
            return false;
        for (int i = 0; i < this.buckets.length; i++) {
            int len = this.buckets[i].length;
            if (len != other.buckets[i].length)
                return false;
            for (int j = 0; j < len; j++)
                if (this.buckets[i][j] != other.buckets[i][j])
                    return false;
        }
        if (!this.histogramMissingX.same(other.histogramMissingX))
            return false;
        if (!this.histogramMissingY.same(other.histogramMissingY))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < this.buckets.length; i++) {
            builder.append(i);
            builder.append(" => ");
            int len = this.buckets[i].length;
            for (int j = 0; j < len; j++) {
                builder.append(j);
                builder.append(" => ");
                builder.append(this.buckets[i][j]);
                builder.append(System.lineSeparator());
            }
            builder.append("missing => ");
            builder.append(this.histogramMissingY.buckets[i]);
            builder.append(System.lineSeparator());
        }

        builder.append("missing => ");
        for (int j = 0; j < this.histogramMissingX.buckets.length; j++) {
            builder.append(j);
            builder.append(" => ");
            builder.append(this.histogramMissingX.buckets[j]);
            builder.append(System.lineSeparator());
        }
        builder.append("missing => ");
        builder.append(this.missingData);
        return builder.toString();
    }
}

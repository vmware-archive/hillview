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
 */

package org.hillview.sketches;
import org.hillview.dataset.api.IJson;
import org.hillview.table.api.*;
import java.io.Serializable;

/**
 * A 2-dimensional histogram.
 */
public class Heatmap implements Serializable, IJson {
    public final long[][] buckets;
    private long missingData; // number of items missing on both columns
    private final IHistogramBuckets bucketDescX;
    private final IHistogramBuckets bucketDescY;
    private Histogram histogramMissingX; // dim1 is missing, dim2 exists
    private Histogram histogramMissingY; // dim2 is missing, dim1 exists
    private long totalSize;

    public Heatmap(final IHistogramBuckets xBuckets,
                   final IHistogramBuckets yBuckets) {
        this.bucketDescX = xBuckets;
        this.bucketDescY = yBuckets;
        this.buckets = new long[xBuckets.getBucketCount()][yBuckets.getBucketCount()];
        // Automatically initialized to 0
        this.histogramMissingX = new Histogram(this.bucketDescY.getBucketCount());
        this.histogramMissingY = new Histogram(this.bucketDescX.getBucketCount());
    }

    public IHistogramBuckets getBucketDescX() { return bucketDescX; }
    public IHistogramBuckets getBucketDescY() { return bucketDescY; }

    public void createHeatmap(final IColumn columnD1, final IColumn columnD2,
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
                    this.histogramMissingY.add(columnD1, currRow, this.bucketDescX);
                } else if (!isMissingD2) {
                    // only column 1 is missing
                    this.histogramMissingX.add(columnD2, currRow, this.bucketDescY);
                } else {
                    // both are missing
                    this.missingData++;
                }
            } else {
                int index1 = this.bucketDescX.indexOf(columnD1, currRow);
                int index2 = this.bucketDescY.indexOf(columnD2, currRow);
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
            this.missingData = (long) ((double) this.missingData / samplingRate);
            for (int i = 0; i < this.buckets.length; i++)
                for (int j = 0; j < this.buckets[i].length; j++)
                    this.buckets[i][j] = (long) ((double) this.buckets[i][j] / samplingRate);
        }
    }

    public Histogram getMissingHistogramD1() { return this.histogramMissingX; }

    public long getSize() { return this.totalSize; }

    public Histogram getMissingHistogramD2() { return this.histogramMissingY; }

    public int getNumOfBucketsD1() { return this.bucketDescX.getBucketCount(); }

    public int getNumOfBucketsD2() { return this.bucketDescY.getBucketCount(); }

    public long getMissingData() { return this.missingData; }

    /**
     * @return the index's count
     */
    public long getCount(final int index1, final int index2) { return this.buckets[index1][index2]; }

    /**
     * @param  otherHeatmap with the same bucketDescriptions
     * @return a new HeatMap which is the union of this and otherHeatmap
     */
    public Heatmap union(Heatmap otherHeatmap) {
        Heatmap unionH = new Heatmap(this.bucketDescX, this.bucketDescY);
        for (int i = 0; i < unionH.bucketDescX.getBucketCount(); i++)
            for (int j = 0; j < unionH.bucketDescY.getBucketCount(); j++)
                unionH.buckets[i][j] = this.buckets[i][j] + otherHeatmap.buckets[i][j];
        unionH.missingData = this.missingData + otherHeatmap.missingData;
        unionH.totalSize = this.totalSize + otherHeatmap.totalSize;
        unionH.histogramMissingX = this.histogramMissingX.union(otherHeatmap.histogramMissingX);
        unionH.histogramMissingY = this.histogramMissingY.union(otherHeatmap.histogramMissingY);
        return unionH;
    }
}

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
public class HeatMap implements Serializable, IJson {
    private final long[][] buckets;
    private long missingData; // number of items missing on both columns
    private long outOfRange;
    private final IBucketsDescription bucketDescX;
    private final IBucketsDescription bucketDescY;
    private Histogram histogramMissingX; // dim1 is missing, dim2 exists
    private Histogram histogramMissingY; // dim2 is missing, dim1 exists
    private long totalSize;

    public HeatMap(final IBucketsDescription xBuckets,
                   final IBucketsDescription yBuckets) {
        this.bucketDescX = xBuckets;
        this.bucketDescY = yBuckets;
        this.buckets = new long[xBuckets.getNumOfBuckets()][yBuckets.getNumOfBuckets()]; // Automatically initialized to 0
        this.histogramMissingX = new Histogram(this.bucketDescY);
        this.histogramMissingY = new Histogram(this.bucketDescX);
    }

    public void createHeatMap(final ColumnAndConverter columnD1, final ColumnAndConverter columnD2,
                              final IMembershipSet membershipSet, double samplingRate, long seed, boolean enforceRate) {
        final ISampledRowIterator myIter = membershipSet.getIteratorOverSample(samplingRate, seed, enforceRate);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            boolean isMissingD1 = columnD1.isMissing(currRow);
            boolean isMissingD2 = columnD2.isMissing(currRow);
            if (isMissingD1 || isMissingD2) {
                if (!isMissingD1) {
                    // only column 2 is missing
                    double val1 = columnD1.asDouble(currRow);
                    this.histogramMissingY.addValue(val1);
                } else if (!isMissingD2) {
                    // only column 1 is missing
                    double val2 = columnD2.asDouble(currRow);
                    this.histogramMissingX.addValue(val2);
                } else {
                    // both are missing
                    this.missingData++;
                }
            } else {
                double val1 = columnD1.asDouble(currRow);
                double val2 = columnD2.asDouble(currRow);
                int index1 = this.bucketDescX.indexOf(val1);
                int index2 = this.bucketDescY.indexOf(val2);
                if ((index1 >= 0) && (index2 >= 0)) {
                    this.buckets[index1][index2]++;
                    this.totalSize++;
                }
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
        samplingRate = myIter.rate();
        if (samplingRate < 1) {
            this.histogramMissingX.rescale(myIter.rate());
            this.histogramMissingY.rescale(myIter.rate());
            this.outOfRange = (long) ((double) this.outOfRange / samplingRate);
            this.missingData = (long) ((double) this.missingData / samplingRate);
            for (int i = 0; i < this.buckets.length; i++)
                for (int j = 0; j < this.buckets[i].length; j++)
                    this.buckets[i][j] = (long) ((double) this.buckets[i][j] / samplingRate);
        }
    }

    public Histogram getMissingHistogramD1() { return this.histogramMissingX; }

    public long getSize() { return this.totalSize; }

    public Histogram getMissingHistogramD2() { return this.histogramMissingY; }

    public int getNumOfBucketsD1() { return this.bucketDescX.getNumOfBuckets(); }

    public int getNumOfBucketsD2() { return this.bucketDescY.getNumOfBuckets(); }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's count
     */
    public long getCount(final int index1, final int index2) { return this.buckets[index1][index2]; }

    /**
     * @param  otherHeatmap with the same bucketDescriptions
     * @return a new HeatMap which is the union of this and otherHeatmap
     */
    public HeatMap union(HeatMap otherHeatmap) {
        HeatMap unionH = new HeatMap(this.bucketDescX, this.bucketDescY);
        for (int i = 0; i < unionH.bucketDescX.getNumOfBuckets(); i++)
            for (int j = 0; j < unionH.bucketDescY.getNumOfBuckets(); j++)
                unionH.buckets[i][j] = this.buckets[i][j] + otherHeatmap.buckets[i][j];
        unionH.missingData = this.missingData + otherHeatmap.missingData;
        unionH.outOfRange = this.outOfRange + otherHeatmap.outOfRange;
        unionH.totalSize = this.totalSize + otherHeatmap.totalSize;
        unionH.histogramMissingX = this.histogramMissingX.union(otherHeatmap.histogramMissingX);
        unionH.histogramMissingY = this.histogramMissingY.union(otherHeatmap.histogramMissingY);
        return unionH;
    }
}

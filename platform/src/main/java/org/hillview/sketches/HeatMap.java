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

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * A 2-dimensional histogram.
 */
public class HeatMap implements Serializable, IJson {
    private final long[][] buckets;
    private long missingData; // number of items missing on both columns
    private long outOfRange;
    private final IBucketsDescription bucketDescDim1;
    private final IBucketsDescription bucketDescDim2;
    private Histogram histogramMissingD1; // hist of items that are missing in D2
    private Histogram histogramMissingD2; // hist of items that are missing in D1
    private long totalSize;

    public HeatMap(final IBucketsDescription buckets1,
                   final IBucketsDescription buckets2) {
        this.bucketDescDim1 = buckets1;
        this.bucketDescDim2 = buckets2;
        this.buckets = new long[buckets1.getNumOfBuckets()][buckets2.getNumOfBuckets()]; // Automatically initialized to 0
        this.histogramMissingD1 = new Histogram(this.bucketDescDim1);
        this.histogramMissingD2 = new Histogram(this.bucketDescDim2);
    }

    /**
     * Creates the histogram explicitly and in full. Should be called at most once.
     */
    public void createHeatMap(final IColumn columnD1, final IColumn columnD2,
                              @Nullable final IStringConverter converterD1,
                              @Nullable final IStringConverter converterD2,
                              final IMembershipSet membershipSet) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            boolean isMissingD1 = columnD1.isMissing(currRow);
            boolean isMissingD2 = columnD2.isMissing(currRow);
            if (isMissingD1 || isMissingD2) {
                if (!isMissingD1)  // only column 2 is missing
                    this.histogramMissingD2.addValue(columnD1.asDouble(currRow, converterD1));
                else if (!isMissingD2) // only column 1 is missing
                    this.histogramMissingD1.addValue(columnD2.asDouble(currRow, converterD2));
                else
                    this.missingData++; // both are missing
            } else {
                double val1 = columnD1.asDouble(currRow, converterD1);
                double val2 = columnD2.asDouble(currRow, converterD2);
                int index1 = this.bucketDescDim1.indexOf(val1);
                int index2 = this.bucketDescDim2.indexOf(val2);
                if ((index1 >= 0) && (index2 >= 0)) {
                    this.buckets[index1][index2]++;
                    this.totalSize++;
                }
                else this.outOfRange++;
            }
            currRow = myIter.getNextRow();
        }
    }

    public Histogram getMissingHistogramD1() { return this.histogramMissingD1; }

    public long getSize() { return this.totalSize; }

    public Histogram getMissingHistogramD2() { return this.histogramMissingD2; }

    public void createSampleHistogram(final IColumn columnD1, final IColumn columnD2,
                                      @Nullable final IStringConverter converterD1,
                                      @Nullable final IStringConverter converterD2,
                                      final IMembershipSet membershipSet, double sampleRate) {
        this.createHeatMap(columnD1, columnD2, converterD1, converterD2, membershipSet.sample(sampleRate));
    }

    public void createSampleHistogram(final IColumn columnD1, final IColumn columnD2,
                                      @Nullable final IStringConverter converterD1,
                                      @Nullable final IStringConverter converterD2,
                                      final IMembershipSet membershipSet,
                                      double sampleRate, long seed) {
        this.createHeatMap(columnD1, columnD2, converterD1, converterD2, membershipSet.sample(sampleRate, seed));
    }

    public int getNumOfBucketsD1() { return this.bucketDescDim1.getNumOfBuckets(); }

    public int getNumOfBucketsD2() { return this.bucketDescDim2.getNumOfBuckets(); }

    public long getMissingData() { return this.missingData; }

    public long getOutOfRange() { return this.outOfRange; }

    /**
     * @return the index's count
     */
    public long getCount(final int index1, final int index2) { return this.buckets[index1][index2]; }

    /**
     * @param  otherHeatmap with the same bucketDescriptions
     * @return a new Histogram which is the union of this and otherHeatmap
     */
    public HeatMap union(HeatMap otherHeatmap) {
        HeatMap unionH = new HeatMap(this.bucketDescDim1, this.bucketDescDim2);
        for (int i = 0; i < unionH.bucketDescDim1.getNumOfBuckets(); i++)
            for (int j = 0; j < unionH.bucketDescDim2.getNumOfBuckets(); j++)
                unionH.buckets[i][j] = this.buckets[i][j] + otherHeatmap.buckets[i][j];
        unionH.missingData = this.missingData + otherHeatmap.missingData;
        unionH.outOfRange = this.outOfRange + otherHeatmap.outOfRange;
        unionH.totalSize = this.totalSize + otherHeatmap.totalSize;
        unionH.histogramMissingD1 = this.histogramMissingD1.union(otherHeatmap.histogramMissingD1);
        unionH.histogramMissingD2 = this.histogramMissingD2.union(otherHeatmap.histogramMissingD2);
        return unionH;
    }
}

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

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.*;
import java.io.Serializable;

/**
 * A 3-dimensional histogram.
 */
public class Heatmap3D implements Serializable, IJson {
    private final long[][][] buckets;
    private long eitherMissing; // number of items missing in either of the columns
    public final int bucketCount0;
    public final int bucketCount1;
    public final int bucketCount2;
    private long totalPresent; // number of items that have no missing values in either column

    public Heatmap3D(final int b0,
                     final int b1,
                     final int b2) {
        this.bucketCount0 = b0;
        this.bucketCount1 = b1;
        this.bucketCount2 = b2;
        this.buckets = new long[b0][b1][b2];
    }

    public void createHeatmap(
            final IColumn col0, final IColumn col1, final IColumn col2,
            final IHistogramBuckets bucket0, IHistogramBuckets bucket1, IHistogramBuckets bucket2,
            final IMembershipSet membershipSet,
            final double samplingRate,
            final long seed, boolean enforceRate) {
        final IRowIterator myIter = membershipSet.getIteratorOverSample(samplingRate, seed, enforceRate);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            boolean isMissingD1 = col0.isMissing(currRow);
            boolean isMissingD2 = col1.isMissing(currRow);
            boolean isMissingD3 = col2.isMissing(currRow);
            if (isMissingD1 || isMissingD2 || isMissingD3) {
                this.eitherMissing++; // At least one of the three is missing.
            } else {
                int index1 = bucket0.indexOf(col0, currRow);
                int index2 = bucket1.indexOf(col1, currRow);
                int index3 = bucket2.indexOf(col2, currRow);
                if ((index1 >= 0) && (index2 >= 0) && (index3 >= 0)) {
                    this.buckets[index1][index2][index3]++;
                    this.totalPresent++;
                }
            }
            currRow = myIter.getNextRow();
        }
    }

    public long getSize() { return this.totalPresent; }

    public long getMissingData() { return this.eitherMissing; }

    public long getCount(final int index1, final int index2, final int index3) {
        return this.buckets[index1][index2][index3];
    }

    /**
     * @param  otherHeatmap3D with the same bucketDescriptions
     * @return a new HeatMap3D which is the union of this and otherHeatmap3D
     */
    public Heatmap3D union(Heatmap3D otherHeatmap3D) {
        Heatmap3D unionH = new Heatmap3D(this.bucketCount0, this.bucketCount1, this.bucketCount2);
        for (int i = 0; i < unionH.bucketCount0; i++)
            for (int j = 0; j < unionH.bucketCount1; j++)
                for (int k = 0; k < unionH.bucketCount2; k++)
                    unionH.buckets[i][j][k] = this.buckets[i][j][k] + otherHeatmap3D.buckets[i][j][k];
        unionH.eitherMissing = this.eitherMissing + otherHeatmap3D.eitherMissing;
        unionH.totalPresent = this.totalPresent + otherHeatmap3D.totalPresent;
        return unionH;
    }
}

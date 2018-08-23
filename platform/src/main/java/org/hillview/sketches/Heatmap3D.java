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
 * A 3-dimensional histogram.
 */
public class Heatmap3D implements Serializable, IJson {
    private final long[][][] buckets;
    private long eitherMissing; // number of items missing in either of the columns
    private final IHistogramBuckets bucketDescDim1;
    private final IHistogramBuckets bucketDescDim2;
    private final IHistogramBuckets bucketDescDim3;
    private long totalPresent; // number of items that have no missing values in either column

    Heatmap3D(final IHistogramBuckets buckets1,
              final IHistogramBuckets buckets2,
              final IHistogramBuckets buckets3) {
        this.bucketDescDim1 = buckets1;
        this.bucketDescDim2 = buckets2;
        this.bucketDescDim3 = buckets3;
        this.buckets = new long[buckets1.getNumOfBuckets()][buckets2.getNumOfBuckets()][buckets3.getNumOfBuckets()];
    }

    void createHeatmap(
            final IColumn columnD1, final IColumn columnD2, final IColumn columnD3,
            final IMembershipSet membershipSet,
            final double samplingRate,
            final long seed, boolean enforceRate) {
        final IRowIterator myIter = membershipSet.getIteratorOverSample(samplingRate, seed, enforceRate);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            boolean isMissingD1 = columnD1.isMissing(currRow);
            boolean isMissingD2 = columnD2.isMissing(currRow);
            boolean isMissingD3 = columnD3.isMissing(currRow);
            if (isMissingD1 || isMissingD2 || isMissingD3) {
                this.eitherMissing++; // At least one of the three is missing.
            } else {
                int index1 = this.bucketDescDim1.indexOf(columnD1, currRow);
                int index2 = this.bucketDescDim2.indexOf(columnD2, currRow);
                int index3 = this.bucketDescDim3.indexOf(columnD3, currRow);
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
        Heatmap3D unionH = new Heatmap3D(this.bucketDescDim1, this.bucketDescDim2, this.bucketDescDim3);
        for (int i = 0; i < unionH.bucketDescDim1.getNumOfBuckets(); i++)
            for (int j = 0; j < unionH.bucketDescDim2.getNumOfBuckets(); j++)
                for (int k = 0; k < unionH.bucketDescDim3.getNumOfBuckets(); k++)
                    unionH.buckets[i][j][k] = this.buckets[i][j][k] + otherHeatmap3D.buckets[i][j][k];
        unionH.eitherMissing = this.eitherMissing + otherHeatmap3D.eitherMissing;
        unionH.totalPresent = this.totalPresent + otherHeatmap3D.totalPresent;
        return unionH;
    }
}

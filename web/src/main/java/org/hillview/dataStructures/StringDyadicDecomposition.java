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

package org.hillview.dataStructures;

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.StringHistogramBuckets;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.utils.Converters;

import java.util.ArrayList;

public class StringDyadicDecomposition extends DyadicDecomposition<String> {
    private String[] leafLeftBoundaries;

    public StringDyadicDecomposition(final String minValue, final String maxValue,
                                     final int bucketCount, StringColumnQuantization metadata) {
        super(minValue, maxValue, metadata.leftBoundaries[0], metadata.globalMax, "a", bucketCount);
        this.leafLeftBoundaries = metadata.leftBoundaries;
        this.globalNumLeaves = this.leafLeftBoundaries.length;
        int numLeaves = this.computeLeafIdx(maxValue) - this.computeLeafIdx(minValue);
        this.bucketLeftBoundaries = new String[this.bucketCount];
        this.init(numLeaves);
    }

    @Override
    protected String leafLeftBoundary(final int leafIdx) {
        if (leafIdx < this.leafLeftBoundaries.length) {
            return this.leafLeftBoundaries[leafIdx];
        } else if (leafIdx == this.leafLeftBoundaries.length) {
            return this.globalMax;
        } else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    @Override
    public ArrayList<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf) {
        int startLeaf = this.bucketLeftLeaves[bucketIdx];
        int endLeaf;
        if (bucketIdx == (this.bucketCount - 1)) { // last bucket
            endLeaf = this.numLeaves;
        } else {
            endLeaf = this.bucketLeftLeaves[bucketIdx+1];
        }

        if (startLeaf >= endLeaf) {
            throw new RuntimeException("Tried to initialize bucket with invalid number of leaves");
        }

        ArrayList<Pair<Integer, Integer>> ret = new ArrayList<Pair<Integer, Integer>>();
        for (int i = startLeaf; i < endLeaf; i++) {
            ret.add(new Pair<Integer, Integer>(i, i+1));
        }

        return ret;
    }

    @Override
    public IHistogramBuckets getHistogramBuckets() {
        return new StringHistogramBuckets(
                Converters.checkNull(this.bucketLeftBoundaries),
                this.maxValue);
    }
}

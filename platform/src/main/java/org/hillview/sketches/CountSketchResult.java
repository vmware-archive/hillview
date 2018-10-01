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

import org.hillview.table.rows.BaseRowSnapshot;
import org.hillview.table.rows.RowSnapshot;

import java.io.Serializable;
import java.util.Arrays;

/**
 * The data structure produced by CountSketch. It consists of an array of doubles of size
 * buckets * trials. These and other parameters are stored in the CountSketchDescription.
 * The data structure can be queried with a RowSnapShot, and will return an estimate of its
 * frequency (using estimateFreq()). It does not directly give the list of the most frequent
 * elements. This is computed in a second phase by using it to run ExactCountSketch.
 */

public class CountSketchResult implements Serializable {
    public CountSketchDescription csDesc;
    public long[][] counts;

    public CountSketchResult(CountSketchDescription csDesc) {
        this.csDesc = csDesc;
        this.counts = new long[csDesc.trials][csDesc.buckets];
    }

    public long estimateFreq(BaseRowSnapshot rss) {
        long item, hash;
        int sign, toBucket;
        long[] estimate = new long[this.csDesc.trials];
        item = rss.hashCode();
        for (int j = 0; j < this.csDesc.trials; j++) {
            hash = this.csDesc.hashFunction[j].hashLong(item);
            sign = (int) hash & 1;
            toBucket = (int) (Math.abs(hash / 2) % this.csDesc.buckets);
            estimate[j] = this.counts[j][toBucket] * sign;
        }
        Arrays.sort(estimate);
        return estimate[this.csDesc.trials/2];
    }

    public double estimateNorm() {
        double[] estimate = new double[this.csDesc.trials];
        for (int i = 0; i < this.csDesc.trials; i++)
            for(int j = 0; j < this.csDesc.buckets; j++)
                estimate[i] += this.counts[i][j]*this.counts[i][j];
        Arrays.sort(estimate);
        return Math.sqrt(estimate[this.csDesc.trials/2]);
    }

    public long[][] getEstimates(RowSnapshot[] rss) {
        long item, hash;
        int sign, toBucket;
        long [][] estimate = new long[rss.length][this.csDesc.trials];
        for (int i = 0; i < rss.length; i++) {
            item = rss[i].hashCode();
            for (int j = 0; j < this.csDesc.trials; j++) {
                hash = this.csDesc.hashFunction[j].hashLong(item);
                sign = (hash % 2 == 0) ? 1 : -1;
                toBucket = (int) (Math.abs(hash / 2) % this.csDesc.buckets);
                estimate[i][j] = this.counts[j][toBucket] * sign;
            }
        }
        return estimate;
    }
}

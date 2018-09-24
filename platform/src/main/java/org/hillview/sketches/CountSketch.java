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

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;

/**
 * In the L_2 heavy-hitters problem, if the input has frequency vector (f_1, ..., f_m), we
 * want a list of all elements such that f_i^2 > epsilon^2*(f_1^2 + ... f_m^2).
 * We solve the L_2 heavy hitters problem by running two sketches sequentially:
 * 1. CountSketch is run in the first phase to return a summary data structure (CountSketchResult)
 * that can be queried to approximate frequencies of any input element.
 * 2. ExactCountSketch is run on the CountSketchResult to compute the top heavy hitters, and their
 * exact frequencies.
 */
public class CountSketch implements ISketch<ITable, CountSketchResult> {
    public CountSketchDescription csDesc;

    public CountSketch(CountSketchDescription csDesc) {
        this.csDesc = csDesc;
    }

    @Override
    public CountSketchResult create(ITable data) {
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.csDesc.schema);
        CountSketchResult result = new CountSketchResult(this.csDesc);
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        long item, hash;
        int sign, toBucket;
        while (i != -1) {
            vrs.setRow(i);
            item = vrs.hashCode();
            for (int j = 0; j < this.csDesc.trials; j++) {
                hash = this.csDesc.hashFunction[j].hashLong(item);
                sign = (int) hash & 1;
                toBucket = (int) (Math.abs(hash/2) % this.csDesc.buckets);
                result.counts[j][toBucket] += sign;
            }
            i = rowIt.getNextRow();
        }
        return result;
    }

    @Nullable
    @Override
    public CountSketchResult zero() {
        return new CountSketchResult(this.csDesc);
    }

    @Nullable
    @Override
    public CountSketchResult add(@Nullable CountSketchResult left, @Nullable CountSketchResult right) {
        CountSketchResult sum = new CountSketchResult(this.csDesc);
        for (int i = 0; i < this.csDesc.trials; i++)
            for (int j = 0; j <  this.csDesc.buckets; j++)
                sum.counts[i][j] = left.counts[i][j] + right.counts[i][j];
        return sum;
    }
}

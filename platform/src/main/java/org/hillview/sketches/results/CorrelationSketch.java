/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.Histogram2DSketch;
import org.hillview.sketches.highorder.MultiSketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.JsonList;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;

public class CorrelationSketch extends MultiSketch<ITable, Groups<Groups<Count>>> {
    static final long serialVersionUID = 1;
    protected final IHistogramBuckets[] buckets;
    protected final long seed;
    protected final double samplingRate;

    static JsonList<ISketch<ITable, Groups<Groups<Count>>>> createSketches(
            IHistogramBuckets[] buckets) {
        JsonList<ISketch<ITable, Groups<Groups<Count>>>> result = new JsonList<>();
        for (int i = 0; i < buckets.length; i++) {
            for (int j = i + 1; j < buckets.length; j++) {
                // swap buckets when passing to Histogram2DSketch
                ISketch<ITable, Groups<Groups<Count>>> sk = new Histogram2DSketch(buckets[j], buckets[i]);
                result.add(sk);
            }
        }
        return result;
    }

    public CorrelationSketch(IHistogramBuckets[] buckets, double samplingRate, long seed) {
        super(createSketches(buckets));
        this.samplingRate = samplingRate;
        this.seed = seed;
        this.buckets = buckets;
    }

    @Override
    public JsonList<Groups<Groups<Count>>> create(@Nullable ITable data) {
        JsonList<String> cols = Linq.mapToList(this.buckets, IHistogramBuckets::getColumn);
        assert data != null;
        IColumn[] columns = data.getLoadedColumns(cols).toArray(new IColumn[0]);
        // Doing it this way is correct but inefficient
        // return super.create(data);

        JsonList<Groups<Groups<Count>>> result = this.zero();
        assert result != null;
        ISampledRowIterator it = data.getMembershipSet().getIteratorOverSample(this.samplingRate, this.seed, false);
        int row = it.getNextRow();
        int[] indexes = new int[this.buckets.length];   // -2 for missing, -1 for out of bounds, bucket index ow.
        while (row >= 0) {
            for (int i = 0; i < this.buckets.length; i++) {
                IColumn col = columns[i];
                if (col.isMissing(row)) {
                    indexes[i] = -2;
                } else {
                    indexes[i] = this.buckets[i].indexOf(col, row);
                }
            }

            int inlist = 0;
            for (int i = 0; i < this.buckets.length; i++) {
                int indexI = indexes[i];
                for (int j = i + 1; j < this.buckets.length; j++, inlist++) {
                    int indexJ = indexes[j];

                    Groups<Groups<Count>> grps = result.get(inlist);
                    Groups<Count> grp;
                    if (indexI < -1) {
                        grp = grps.perMissing;
                    } else if (indexI == -1) {
                        continue;
                    } else {
                        grp = grps.perBucket.get(indexI);
                    }
                    if (grp != null) {
                        if (indexJ < -1) {
                            grp.perMissing.count++;
                        } else if (indexJ >= 0) {
                            grp.getBucket(indexJ).add(1);
                        } // else continue;
                    }
                }
            }
            row = it.getNextRow();
        }

        return result;
    }
}

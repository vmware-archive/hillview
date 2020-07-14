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

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;

@SuppressWarnings("CanBeFinal")
public class QuantilesArgs {
    // if this is String, or Json we are sampling strings
    public ColumnDescription cd = new ColumnDescription();
    public long seed;       // only used if sampling strings
    public int stringsToSample;  // only used if sampling strings

    public PostProcessedSketch<ITable, BucketsInfo, BucketsInfo> getPostSketch() {
        ISketch<ITable, BucketsInfo> res;
        if (this.cd.kind.isString()) {
            int samples = Math.min(this.stringsToSample * this.stringsToSample, 100000);
            ISketch<ITable, MinKSet<String>> s = new SampleDistinctElementsSketch(
                    // We sample stringsToSample squared
                    this.cd.name, this.seed, samples);
            @SuppressWarnings("unchecked")
            ISketch<ITable, BucketsInfo> result = (ISketch<ITable, BucketsInfo>)(Object)s;
            res = result;
        } else if (this.cd.kind == ContentsKind.Interval) {
            ISketch<ITable, DataRange> s = new IntervalDataRangeSketch(this.cd.name);
            @SuppressWarnings("unchecked")
            ISketch<ITable, BucketsInfo> result = (ISketch<ITable, BucketsInfo>)(Object)s;
            res = result;
        } else {
            ISketch<ITable, DataRange> s = new DoubleDataRangeSketch(this.cd.name);
            @SuppressWarnings("unchecked")
            ISketch<ITable, BucketsInfo> result = (ISketch<ITable, BucketsInfo>)(Object)s;
            res = result;
        }
        return res.andThen(result -> {
                BucketsInfo bi = result;
                if (QuantilesArgs.this.cd.kind.isString()) {
                    int b = QuantilesArgs.this.stringsToSample;
                    @SuppressWarnings("unchecked")
                    MinKSet<String> mks = (MinKSet<String>) result;
                    assert mks != null;
                    bi = new StringQuantiles(
                            mks.getLeftBoundaries(b), mks.max, mks.allStringsKnown(b),
                            mks.presentCount, mks.missingCount);
                }
                return bi;
            });
    }
}

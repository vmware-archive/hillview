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

package org.hillview.dataset;

import org.hillview.dataset.IncrementalTableSketch;
import org.hillview.dataset.TableSketch;
import org.hillview.dataset.api.IScalable;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * Runs an incremental sketch over a table with specified sampling parameters.
 * @param <R>  Result produced by the original and this sketch.
 * @param <S>  Actual sketch that will be run.
 */
public class SamplingTableSketch<
        SW extends ISketchWorkspace,
        R extends ISketchResult & IScalable<R>,
        S extends IncrementalTableSketch<R, SW>>
    implements TableSketch<R> {
    protected final double samplingRate;
    protected final long seed;
    protected final S actualSketch;

    public SamplingTableSketch(double samplingRate, long seed, S actualSketch) {
        this.samplingRate = samplingRate;
        this.seed = seed;
        this.actualSketch = actualSketch;
    }

    @Override
    public R create(@Nullable ITable data) {
        R result = Converters.checkNull(this.actualSketch.zero());
        SW workspace = this.actualSketch.initialize(Converters.checkNull(data));
        ISampledRowIterator it = data
                .getMembershipSet()
                .getIteratorOverSample(this.samplingRate, this.seed, false);
        int row = it.getNextRow();
        while (row >= 0) {
            this.actualSketch.add(workspace, result, row);
            row = it.getNextRow();
        }
        return result.rescale(this.samplingRate);
    }

    @Nullable
    @Override
    public R zero() {
        return this.actualSketch.zero();
    }

    @Nullable
    @Override
    public R add(@Nullable R left, @Nullable R right) {
        return this.actualSketch.add(left, right);
    }
}

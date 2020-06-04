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

package org.hillview.dataset.api;

import org.hillview.sketches.highorder.SamplingTableSketch;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ISketchWorkspace;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * An incremental table sketch can update the result for each table row.
 * @param <R>  Result produced by the sketch.
 */
public abstract class IncrementalTableSketch<
        R extends ISketchResult & IScalable<R>,
        W extends ISketchWorkspace>
        implements TableSketch<R> {
    /**
     * Add to the result the data in the specified row number.
     * @param result     Result to add to.
     * @param rowNumber  Row number in the table.
     */
    public abstract void add(W workspace, R result, int rowNumber);

    /**
     * Allocates a workspace for a sketch, that can later
     * be passed to add.
     * @param data  Data that will be processed.
     */
    public abstract W initialize(ITable data);

    /**
     * This function is invoked after computing a sketch over a sampled data
     * source to adjust the estimated counts.
     * @param samplingRate  Sampling rate that was used; between 0 and 1.
     * @param result        Sketch result that has to be scaled.
     */
    public R rescale(R result, double samplingRate) {
        return result.rescale(samplingRate);
    }

    @Override
    public R create(@Nullable ITable data) {
        R result = Converters.checkNull(this.zero());
        W workspace = this.initialize(Converters.checkNull(data));
        IRowIterator it = data.getRowIterator();
        int row = it.getNextRow();
        while (row >= 0) {
            this.add(workspace, result, row);
            row = it.getNextRow();
        }
        return result;
    }

    /**
     * A sampled version of this sketch.
     * @param samplingRate  Sampling rate.
     * @param seed          Seed for sampling.
     * @return              This sketch running over sampled data.
     */
    public TableSketch<R> sampled(double samplingRate, long seed) {
        return new SamplingTableSketch<>(Converters.checkRate(samplingRate), seed, this);
    }
}

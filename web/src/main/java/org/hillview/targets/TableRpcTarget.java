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

package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataStructures.IntervalDecomposition;
import org.hillview.dataStructures.NumericIntervalDecomposition;
import org.hillview.dataStructures.StringIntervalDecomposition;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.TableSketch;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.results.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.QuantizationSchema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * An RPC target that represents an ITable.
 * The main role is to make sure that targets that handle table
 * use the same method prototypes.
 */
public abstract class TableRpcTarget extends RpcTarget {
    static final long serialVersionUID = 1;
    protected IDataSet<ITable> table;

    protected TableRpcTarget(HillviewComputation computation) {
        super(computation);
        // We expect that this will be soon overwritten by a call
        // to setTable.
        this.table = new LocalDataSet<>(new SmallTable());
    }

    void setTable(IDataSet<ITable> table) {
        this.table = table;
    }

    /**
     * Class used to deserialize JSON request from UI for a histogram.
     */
    public static class HistogramRequestInfo {
        public ColumnDescription cd = new ColumnDescription();
        public double samplingRate;
        public long seed;
        // Only used when doing string histograms
        @Nullable
        private String[] leftBoundaries;
        // Only used when doing double histograms
        private double min;
        private double max;
        private int bucketCount;

        public HistogramRequestInfo() {}

        /**
         * Explicit constructors for headless testing.
         */
        public HistogramRequestInfo(ColumnDescription cd, long seed, double min, double max, int bucketCount) {
            this.cd = cd;
            this.seed = seed;
            this.min = min;
            this.max = max;
            this.bucketCount = bucketCount;
            this.samplingRate = 1.0;
        }

        public HistogramRequestInfo(ColumnDescription cd, long seed, String[] leftBoundaries) {
            this.cd = cd;
            this.seed = seed;
            this.leftBoundaries = leftBoundaries;
            this.samplingRate = 1.0;
        }

        public IHistogramBuckets getBuckets(@Nullable ColumnQuantization quantization) {
            if (cd.kind.isString()) {
                Converters.checkNull(this.leftBoundaries);
                if (quantization != null)
                    return new StringHistogramBuckets(this.cd.name, this.leftBoundaries,
                            ((StringColumnQuantization)quantization).globalMax);
                else
                    return new StringHistogramBuckets(this.cd.name, this.leftBoundaries);
            } else {
                return new DoubleHistogramBuckets(this.cd.name, this.min, this.max, this.bucketCount);
            }
        }

        public IHistogramBuckets getBuckets() {
            return this.getBuckets(null);
        }

        public TableSketch<Groups<Count>> getSketch(@Nullable ColumnQuantization quantization) {
            IHistogramBuckets buckets = this.getBuckets(quantization);
            HistogramSketch sk = new HistogramSketch(buckets);
            if (quantization == null) {
                if (this.samplingRate < 1)
                    return sk.sampled(this.samplingRate, this.seed);
                return sk;
            }
            assert this.samplingRate >= 1;
            return sk.quantized(new QuantizationSchema(quantization));
        }

        public TableSketch<Groups<Count>> getSketch() {
            return this.getSketch(null);
        }

        public IntervalDecomposition getDecomposition(ColumnQuantization quantization) {
            IHistogramBuckets buckets = this.getBuckets(quantization);
            if (cd.kind.isString()) {
                return new StringIntervalDecomposition(
                        (StringColumnQuantization)quantization,
                        (StringHistogramBuckets)buckets);
            } else {
                return new NumericIntervalDecomposition(
                        (DoubleColumnQuantization)quantization,
                        (DoubleHistogramBuckets)buckets);
            }
        }
    }
}

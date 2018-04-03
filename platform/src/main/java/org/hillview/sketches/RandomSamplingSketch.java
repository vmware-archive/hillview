/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.HashSubSchema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This sketch selects random rows from the table, and gives as a result a SmallTable. The rows in the table are
 * the randomly selected rows, and the columns are the numeric columns specified in the constructor. The number of
 * samples is specified in the constructor too.
 */
public class RandomSamplingSketch implements ISketch<ITable, SmallTable> {
    public static final double overSampling = 1.5;
    private final double rate;
    private final long seed;
    private final String[] columnNames;
    private final boolean allowMissing;

    /**
     * Sample the table with the given rate and columns.
     * The result is a SmallTable with the given columns.
     * @param rate The rate in which the table is sampled. To account for rounding errors, the table is oversampled
     *             by a factor of RandomSamplingSketch.overSampling. To have an exact sampling, the result's
     *             membership set should be sampled once more.
     * @param columnNames The list of columns that will be in the resulting SmallTable.  If empty
     *                    all columns are used.
     * @param allowMissing If this is false, do not allow rows that have missing values in at least one of the
     *                     columns to be in the result.
     */
    public RandomSamplingSketch(double rate, long seed, String[] columnNames, boolean allowMissing) {
        this.rate = Math.min(overSampling * rate, 1.0);
        this.seed = seed;
        this.columnNames = columnNames;
        this.allowMissing = allowMissing;
    }

    /**
     * Sample the table with the given rate, using all columns and allowing missing values.
     * The result is a table with the same columns as the input.
     * @param rate The rate in which the table is sampled. To account for rounding errors, the table is oversampled
     *             by a factor of RandomSamplingSketch.overSampling. To have an exact sampling, the result's
     *             membership set should be sampled once more.
     */
    public RandomSamplingSketch(double rate, long seed) {
        this(rate, seed, new String[0], true);
    }

    @Override
    public SmallTable create(ITable data) {
        List<ColumnAndConverterDescription> ccds;
        if (this.columnNames.length != 0)
            ccds = ColumnAndConverterDescription.create(this.columnNames);
        else
            ccds = ColumnAndConverterDescription.create(data.getSchema().getColumnNames());
        List<ColumnAndConverter> cols = data.getLoadedColumns(ccds);

        IMembershipSet sample;
        if (this.allowMissing) {
            sample = data.getMembershipSet().sample(this.rate, this.seed);
        } else {
            sample = data.getMembershipSet().filter((row) -> {
                for (ColumnAndConverter column : cols) {
                    if (column.isMissing(row))
                        return false;
                }
                return true;
            }).sample(this.rate, this.seed);
        }
        ISubSchema schema = new HashSubSchema(this.columnNames);
        return data.compress(schema, sample);
    }

    @Override
    public SmallTable zero() {
        return new SmallTable();
    }

    @Override
    public SmallTable add(@Nullable SmallTable left, @Nullable SmallTable right) {
        assert left != null;
        assert right != null;
        if (left.getNumOfRows() > 0) {
            return left.concatenate(right);
        } else if (right.getNumOfRows() > 0) {
            return right.concatenate(left);
        } else {
            return new SmallTable();
        }
    }
}

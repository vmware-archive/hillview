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
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.List;

/**
 * A sketch to compute correlations between columns, using sampling.
 */
public class SampleCorrelationSketch implements ISketch<ITable, CorrMatrix> {
    /**
     * The list of columns whose correlations we wish to compute. The columns are must be of type
     * Int or Double.
     */
    private final String[] colNames;
    private final long seed;
    private final double samplingRate;

    public SampleCorrelationSketch(String[] colNames, double samplingRate, long seed) {
        this.colNames= colNames;
        this.samplingRate = samplingRate;
        this.seed = seed;
    }

    /**
     * The default probability of a row being included in the sample is 0.1
     */
    public SampleCorrelationSketch(String[] colNames, long seed) {
        this(colNames, .1, seed);
    }

    @Nullable
    @Override
    public CorrMatrix zero() {
        return new CorrMatrix(this.colNames);
    }

    @Nullable
    @Override
    public CorrMatrix add(@Nullable CorrMatrix left, @Nullable CorrMatrix right) {
        assert left != null;
        assert right != null;
        for (int i = 0; i < this.colNames.length; i++)
            for (int j = i; j < this.colNames.length; j++)
                left.update(i, j, right.get(i,j));
        left.count += right.count;
        return left;
    }

    /**
     * We sample rows from the table with probability given by the sampling rate. We then compute
     * the exact correlation for the sampled Table.
     * @param data  Data to sketch.
     * @return A correlation matrix computed over the sampled table.
     */
    @Override
    public CorrMatrix create(@Nullable ITable data) {
        Converters.checkNull(data);
        for (String col : this.colNames) {
            if ((data.getSchema().getKind(col) != ContentsKind.Double) &&
                    (data.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Correlation Sketch requires column to be " +
                        "integer or double: " + col);
        }

        List<IColumn> iCols = data.getLoadedColumns(this.colNames);
        CorrMatrix cm = new CorrMatrix(this.colNames);
        IMembershipSet mm = data.getMembershipSet();
        IRowIterator rowIt = mm.getIteratorOverSample(this.samplingRate, this.seed, true);
        int i = rowIt.getNextRow();
        double valJ, valK;
        while (i != -1) {
            for (int j = 0; j < this.colNames.length; j++) {
                valJ = iCols.get(j).asDouble(i);
                cm.update(j, j, valJ * valJ);
                for (int k = j + 1; k < this.colNames.length; k++) {
                    valK = iCols.get(k).asDouble(i);
                    cm.update(j, k, valJ * valK);
                }
            }
            i = rowIt.getNextRow();
            cm.count++;
        }
        return cm;
    }
}

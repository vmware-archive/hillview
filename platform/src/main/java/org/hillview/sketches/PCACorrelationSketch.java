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
import org.hillview.utils.BlasConversions;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;

/**
 * This class computes the correlations between different columns in the table.
 * This class is very similar to the SampleCorrelationSketch, except that it handles missing values
 * carefully (it does not include them while computing expectations).
 */

public class PCACorrelationSketch implements ISketch<ITable, CorrMatrix> {
    private final String[] colNames;
    private final long seed;
    private final double samplingRate;

    public PCACorrelationSketch(String[] colNames, long totalRows, long seed) {
        this.colNames= colNames;
        this.samplingRate = Math.min(1, 10000.0*colNames.length/totalRows);
        this.seed = seed;
    }

    public PCACorrelationSketch(String[] colNames) {
        this.colNames= colNames;
        this.samplingRate = 1;
        this.seed = 0;
    }

    @Override
    public CorrMatrix create(ITable data) {
        for (String col : this.colNames) {
            if ((data.getSchema().getKind(col) != ContentsKind.Double) &&
                    (data.getSchema().getKind(col) != ContentsKind.Integer))
                throw new InvalidParameterException("Correlation Sketch requires column to be " +
                        "integer or double: " + col);
        }
        CorrMatrix corrMatrix = new CorrMatrix(this.colNames);
        ITable table;
        if (this.samplingRate >= 1)
            table = data;
        else {
            List<ColumnAndConverterDescription> ccds = ColumnAndConverterDescription.create(
                    this.colNames);
            List<ColumnAndConverter> iCols = data.getLoadedColumns(ccds);
            IMembershipSet mm = data.getMembershipSet().sample(this.samplingRate, this.seed);
            table = data.compress(mm);
        }
        int nRows = table.getNumOfRows();
        int nCols = this.colNames.length;
        // Convert the columns to a DoubleMatrix.
        DoubleMatrix mat = BlasConversions.toDoubleMatrix(table, Arrays.asList(this.colNames));

        // The number of non-missing values per column pair
        corrMatrix.nonMissing = DoubleMatrix.ones(nCols, nCols).mul(nRows);
        for (int row = 0; row < mat.rows; row++) {
            for (int i = 0; i < mat.columns; i++) {
                if (Double.isNaN(mat.get(row, i))) {
                    mat.put(row, i, 0); // Set the value to 0 so it doesn't contribute.
                    corrMatrix.nonMissing.put(i, i, corrMatrix.nonMissing.get(i, i) - 1);
                    for (int j = i; j < mat.columns; j++) {
                        if (Double.isNaN(mat.get(row, j))) {
                            corrMatrix.nonMissing.put(i, j, corrMatrix.nonMissing.get(i, j) - 1);
                            corrMatrix.nonMissing.put(j, i, corrMatrix.nonMissing.get(j, i) - 1);
                        }
                    }
                }
            }
        }

        // Since the missing values are set to 0, they don't contribute to the covariance matrix.
        DoubleMatrix covMat = mat.transpose().mmul(mat);

        // Normalize by the number of *actual* values processed. (Also for the mean!)
        covMat.divi(corrMatrix.nonMissing);
        DoubleMatrix means = mat.columnSums().divRowVector(corrMatrix.nonMissing.diag());

        for (int i = 0; i < this.colNames.length; i++) {
            for (int j = i; j < this.colNames.length; j++) {
                corrMatrix.put(i, j,  covMat.get(i, j));
            }
            corrMatrix.means[i] = means.get(i);
        }
        return corrMatrix;
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
        CorrMatrix result = new CorrMatrix(this.colNames);

        for (int i = 0; i < this.colNames.length; i++) {
            if (left.nonMissing.get(i, i) + right.nonMissing.get(i, i) == 0)
                result.means[i] = 0;
            else {
                double meanAlpha = left.nonMissing.get(i, i) / (left.nonMissing.get(i, i) + right.nonMissing.get(i, i));
                result.means[i] = meanAlpha * left.means[i] + (1 - meanAlpha) * right.means[i];
            }
            for (int j = i; j < this.colNames.length; j++) {
                if (left.nonMissing.get(i, j) + right.nonMissing.get(i, j) == 0) {
                    result.put(i, j, 0);
                } else {
                    double alpha = left.nonMissing.get(i, j) / (left.nonMissing.get(i, j) + right.nonMissing.get(i, j));
                    double val = alpha * left.get(i, j) + (1 - alpha) * right.get(i, j);
                    result.put(i, j, val);
                }
            }
        }
        result.nonMissing = left.nonMissing.add(right.nonMissing);

        return result;
    }
}

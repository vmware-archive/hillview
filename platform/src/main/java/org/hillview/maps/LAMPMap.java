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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.ColumnDescription;

import org.hillview.table.api.*;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.SparseColumn;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.jblas.Singular;

/**
 * This map receives a set of high- and low-dimensional control points, and computes a mapping of the rest of the
 * point in the table. For every row x in the table, a transformation is sought that maps nD control points close x,
 * also close to each other in low-dimensional space. More details in:
 *   Joia, Paulo, et al. "Local affine multidimensional projection."
 *   IEEE Transactions on Visualization and Computer Graphics 17.12 (2011): 2563-2571
 *   https://doi.org/10.1109/TVCG.2011.220
 */
public class LAMPMap implements IMap<ITable, ITable> {
    private final static double eps = 1e-9;
    private final String[] numColNames;
    private final DoubleMatrix highDimControlPoints;
    private final DoubleMatrix lowDimControlPoints;
    private final int highDims;
    private final int lowDims;
    private final String[] newColNames;

    public LAMPMap(DoubleMatrix highDimControlPoints, DoubleMatrix lowDimControlPoints,
                   String[] numColNames, String[] newColNames) {
        this.numColNames = numColNames;
        this.highDimControlPoints = highDimControlPoints;
        this.lowDimControlPoints = lowDimControlPoints;
        this.lowDims = lowDimControlPoints.columns;
        this.highDims = highDimControlPoints.columns;
        this.newColNames = newColNames;
    }

    @Override
    public ITable apply(ITable data) {
        ColumnAndConverterDescription[] ccds =
                ColumnAndConverterDescription.create(this.numColNames);
        ColumnAndConverter[] columns = data.getLoadedColumns(ccds);

        IMutableColumn[] newColumns = new IMutableColumn[this.lowDims];
        IMembershipSet set = data.getMembershipSet();
        int colSize = set.getMax();
        for (int i = 0; i < this.lowDims; i++) {
            ColumnDescription cd = new ColumnDescription(
                    this.newColNames[i], ContentsKind.Double, true);
            if (set.useSparseColumn(set.getSize()))
                newColumns[i] = new SparseColumn(cd, colSize);
            else
                newColumns[i] = new DoubleArrayColumn(cd, colSize);
        }

        IRowIterator rowIt = data.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            DoubleMatrix x = new DoubleMatrix(1, this.highDims);
            boolean missing = false;
            for (int i = 0 ; i < this.highDims; i++) {
                if (columns[i].isMissing(row)){
                    missing = true;
                    break;
                }
                else
                    x.put(i, columns[i].asDouble(row));
            }
            if (!missing) {
                DoubleMatrix y = computeMapping(x);

                for (int i = 0; i < y.columns; i++) {
                    if (Double.isNaN(y.get(i))) {
                        missing = true;
                        break;
                    }
                }
                if (!missing) {
                    for (int i = 0; i < this.lowDims; i++) {
                        newColumns[i].set(row, y.get(i));
                    }
                } else {
                    for (int i = 0; i < this.lowDims; i++) {
                        newColumns[i].setMissing(row);
                    }
                }
            }
            row = rowIt.getNextRow();
        }

        return data.append(newColumns);
    }

    private DoubleMatrix computeMapping(DoubleMatrix x) {
        DoubleMatrix alphas = MatrixFunctions.pow(
                MatrixFunctions.pow(
                        this.highDimControlPoints.subRowVector(x),
                        2
                ).rowSums(),
                -1
        );
        double alpha = alphas.sum();

        DoubleMatrix xTilde = this.highDimControlPoints.mulColumnVector(alphas).columnSums().div(alpha);
        DoubleMatrix yTilde = this.lowDimControlPoints.mulColumnVector(alphas).columnSums().div(alpha);
        DoubleMatrix xHats = this.highDimControlPoints.subRowVector(xTilde);
        @SuppressWarnings("SuspiciousNameCombination")
        DoubleMatrix yHats = this.lowDimControlPoints.subRowVector(yTilde);

        DoubleMatrix sqrtAlphas = MatrixFunctions.sqrt(alphas);
        DoubleMatrix A = xHats.mulColumnVector(sqrtAlphas);
        DoubleMatrix B = yHats.mulColumnVector(sqrtAlphas);

        DoubleMatrix[] svdComposition = Singular.sparseSVD(A.transpose().mmul(B));
        DoubleMatrix U = svdComposition[0];
        DoubleMatrix V = svdComposition[2];

        DoubleMatrix M = U.mmul(V);

        return x.sub(xTilde).mmul(M).add(yTilde);
    }
}
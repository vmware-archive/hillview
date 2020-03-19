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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.SparseColumn;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.Converters;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This map takes a list of column names and a projection matrix and applies the projection matrix to the matrix that is
 * constructed from the table by horizontally stacking the specified columns. The resulting table is a copy of the old
 * table, with the additional projected columns added to it. The new columns are named '{newColName}{i}'.
 */
public class LinearProjectionMap implements IMap<ITable, ITable> {
    static final long serialVersionUID = 1;
    /**
     * The projection matrix is structured as follows: Every row is a vector
     * that is projected on. The ordering of the columns is the same as the order
     * of the column names in colNames.
     */
    private final DoubleMatrix projectionMatrix;
    private final String[] colNames;
    private final String[] newColNames;
    private final int numProjections;
    // For columns sparser than this use sparse storage.

    public LinearProjectionMap(String[] colNames, DoubleMatrix projectionMatrix, String projectionName) {
        if (colNames.length != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.newColNames = new String[this.projectionMatrix.rows];
        for (int i = 0; i < projectionMatrix.rows; i++)
            newColNames[i] = projectionName + i;
        this.numProjections = projectionMatrix.rows;
    }

    public LinearProjectionMap(String[] colNames, DoubleMatrix projectionMatrix, String[] newColNames) {
        if (colNames.length != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.newColNames = newColNames;
        this.numProjections = projectionMatrix.rows;
    }

    @Override
    public ITable apply(@Nullable ITable table) {
        List<IColumn> columns = new ArrayList<IColumn>(this.newColNames.length);
        // Compute the projection with BLAS
        DoubleMatrix mat = BlasConversions.toDoubleMatrix(
                Converters.checkNull(table), Arrays.asList(this.colNames));
        DoubleMatrix resultMat = mat.mmul(this.projectionMatrix.transpose());

        // Copy the result to new columns with the same membershipSet size. (Can't use
        // BlasConversions here.)
        for (int j = 0; j < this.numProjections; j++) {
            ColumnDescription colDesc = new ColumnDescription(
                    this.newColNames[j], ContentsKind.Double);
            IMembershipSet set = table.getMembershipSet();
            int colSize = table.getMembershipSet().getMax();
            IMutableColumn column;
            if (set.useSparseColumn(set.getSize()))
                column = new SparseColumn(colDesc, colSize);
            else
                column = new DoubleArrayColumn(colDesc, colSize);
            IRowIterator it = table.getMembershipSet().getIterator();
            int row = it.getNextRow();
            int i = 0;
            while (row >= 0) {
                if (Double.isNaN(resultMat.get(i, j))) {
                    column.setMissing(row);
                } else {
                    column.set(row, resultMat.get(i, j));
                }
                row = it.getNextRow();
                i++;
            }
            columns.add(column);
        }

        return table.append(columns);
    }
}

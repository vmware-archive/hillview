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

package org.hillview.utils;

import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.PointRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class contains methods that convert from/to Tables to/from DoubleMatrices. These methods copy all data, so they
 * should only be used when it is known that the Table is reasonably small.
 */
public class BlasConversions {
    /**
     * Convert from an ITable to a DoubleMatrix. This copies all data from the table.
     * @param table Table that is to be converted.
     * @param colNames Names of columns in the table that have to be converted.
     * @return DoubleMatrix with the table's columns interpreted as doubles.
     */
    public static DoubleMatrix toDoubleMatrix(ITable table, List<String> colNames) {
        List<IColumn> cols = table.getLoadedColumns(colNames);
        DoubleMatrix mat = new DoubleMatrix(table.getNumOfRows(), colNames.size());
        for (int j = 0; j < colNames.size(); j++) {
            IColumn col = cols.get(j);
            IRowIterator iter = table.getRowIterator();
            int row = iter.getNextRow();
            int i = 0;
            while (row >= 0) {
                if (col.isMissing(row)) {
                    mat.put(i, j, Double.NaN);
                } else {
                    mat.put(i, j, col.asDouble(row));
                }
                row = iter.getNextRow();
                i++;
            }
        }
        return mat;
    }

    /**
     * Convert from a DoubleMatrix to a Table. This copies all the data.
     * @param mat Matrix with numeric data that has to be in the table.
     * @param colNames A list with the column names of the newly created table.
     * @return Table with the numeric data from mat. Column names are set to '{columnNames[i]}'.
     */
    private static Table toTable(DoubleMatrix mat, List<String> colNames) {
        IColumn[] columns = new IColumn[mat.columns];
        for (int i = 0; i < mat.columns; i++) {
            ColumnDescription cd = new ColumnDescription(colNames.get(i), ContentsKind.Double);
            DoubleMatrix vector = mat.get(new AllRange(), new PointRange(i));
            IColumn column = new DoubleArrayColumn(cd, vector.data);
            columns[i] = column;
        }
        return new Table(Arrays.asList(columns), null, null);
    }

    /**
     * Calls the above toTable method with column names 'Column{i}'.
     * @param mat Matrix that has to be converted to a table.
     * @return Table with the numeric data from mat. Column names are set to 'Column{i}'.
     */
    public static Table toTable(DoubleMatrix mat) {
        List<String> colNames = new ArrayList<String>();
        for (int i = 0; i < mat.columns; i++) {
            colNames.add("Column" + i);
        }
        return toTable(mat, colNames);
    }

}

/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.test.dataStructures;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.Table;
import org.hillview.test.BaseTest;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

@SuppressWarnings("ConstantConditions")
public class MatrixTest extends BaseTest {
    @Test
    public void testSimpleMatrix() {
        // Setting elements
        DoubleMatrix mat = new DoubleMatrix(2, 2);
        mat.put(0, 0, 2.3);
        mat.put(1, 0, 42.42);
        mat.put(0, 1, 3.14);
        mat.put(1, 1, 3.91992);
        Assert.assertEquals(mat.get(0, 0), 2.3, Math.ulp(2.3));
        Assert.assertEquals(mat.get(1, 0), 42.42, Math.ulp(42.42));
        Assert.assertEquals(mat.get(0, 1), 3.14, Math.ulp(3.14));
        Assert.assertEquals(mat.get(1, 1), 3.91992, Math.ulp(3.91992));

        // Matrix multiplied with its transpose should be symmetric.
        DoubleMatrix symMat = mat.mmul(mat.transpose());
        for (int i = 0; i < symMat.rows; i++) {
            for (int j = 0; j < symMat.columns; j++) {
                Assert.assertEquals(symMat.get(i, j), symMat.get(j, i), Math.ulp(symMat.get(i, j)));
            }
        }
    }

    @Test
    public void testMatrixFetch() {
        ITable table = TestTables.getIntTable(100, 3);
        List<String> colNames = new ArrayList<String>(2);
        colNames.add("Column0");
        colNames.add("Column1");

        DoubleMatrix mat = BlasConversions.toDoubleMatrix(table, colNames);
        IRowIterator it = table.getRowIterator();
        int row = it.getNextRow();
        int i = 0;
        List<IColumn> cols = table.getLoadedColumns(colNames);

        while (row >= 0) {
            for (int j = 0; j < colNames.size(); j++) {
                Assert.assertEquals(
                        mat.get(i, j),
                        cols.get(j).asDouble(row),
                        Math.ulp(mat.get(i, j))
                );
            }
            row = it.getNextRow();
            i++;
        }
        Assert.assertEquals(i, mat.rows);
    }

    @Test
    public void testMissingConversion() {
        int numCols = 5;
        int[] missing = {3, 5, 12, 20, 0};
        int numRows = 30;

        List<IColumn> columns = new ArrayList<IColumn>();
        for (int i = 0; i < numCols; i++) {
            ColumnDescription description = new ColumnDescription("Column" + i, ContentsKind.Double);
            DoubleArrayColumn column = new DoubleArrayColumn(description, numRows);
            for (int j = 0; j < missing[i]; j++) {
                column.setMissing(j);
            }
            columns.add(column);
        }

        ITable table = new Table(columns, null, null);
        List<String> colNames = table.getSchema().getColumnNames();
        DoubleMatrix mat = BlasConversions.toDoubleMatrix(table, colNames);
        DoubleMatrix missingCount = mat.isNaN().columnSums();
        for (int i = 0; i < numCols; i++) {
            Assert.assertEquals(missing[i], Math.round(missingCount.get(i)));
        }
    }
}

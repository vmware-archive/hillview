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
 *
 */

package org.hiero.sketch;

import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.TestTables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hiero.sketch.DoubleArrayTest.generateDoubleArray;
import static org.hiero.utils.IntArrayGenerator.generateIntArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableTest {
    @Test
    public void getTableTest(){
        final SmallTable leftTable = TestTables.getIntTable(100, 2);
        assertNotNull(leftTable);
    }

    @Test
    public void columnCompressTest() {
        final int size = 100;
        final IntArrayColumn col = generateIntArray(size);
        final FullMembership FM = new FullMembership(size);
        final IMembershipSet PMD = FM.filter(row -> (row % 2) == 0);
        final IColumn smallCol = col.compress(PMD);
        assertNotNull(smallCol);
    }

    @Test
    public void tableTest0() {
        final int size = 100;
        final int numCols =2;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(generateIntArray(size));
        columns.add(generateDoubleArray(size));
        FullMembership full = new FullMembership(size);
        IMembershipSet partial = full.filter(row -> (row % 2) == 0);
        Table myTable = new Table(columns, partial);
        assertEquals(myTable.toString(), "Table, 2 columns, 50 rows");
        ITable smallTable = myTable.compress();
        assertEquals(smallTable.toString(), "Table, 2 columns, 50 rows");
    }

    @Test
    public void tableTest1() {
        final int size = 100;
        final int numCols =2;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(generateIntArray(size));
        columns.add(generateDoubleArray(size));
        final FullMembership full = new FullMembership(size);
        final IMembershipSet partial = full.filter(row -> (row % 2) == 0);
        final Table myTable = new Table(columns, partial);
        assertEquals(myTable.toString(), "Table, 2 columns, 50 rows");
        HashSubSchema filter = new HashSubSchema();
        filter.add(columns.get(1).getDescription().name);
        ITable smallTable = myTable.compress(filter, partial);
        assertEquals(smallTable.toString(), "Table, 1 columns, 50 rows");
    }
}
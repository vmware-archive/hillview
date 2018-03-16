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

package org.hillview.test;

import org.hillview.table.*;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.utils.IntArrayGenerator;
import org.hillview.utils.TestTables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hillview.test.DoubleArrayTest.generateDoubleArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableTest extends BaseTest {
    @Test
    public void getTableTest() {
        final SmallTable leftTable = TestTables.getIntTable(100, 2);
        assertNotNull(leftTable);
    }

    @Test
    public void columnCompressTest() {
        final int size = 100;
        final int numCols = 3;
        final IntArrayColumn col = IntArrayGenerator.getMissingIntArray("X", size, 5);
        final FullMembershipSet FM = new FullMembershipSet(size);
        final IMembershipSet PMD = FM.filter(row -> (row % 2) == 0);
        final IColumn smallCol = col.compress(PMD);
        assertNotNull(smallCol);
    }

    @Test
    public void tableTest0() {
        final int size = 100;
        final int numCols =2;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(IntArrayGenerator.getMissingIntArray("X", size, 5));
        columns.add(generateDoubleArray(size, 100));
        FullMembershipSet full = new FullMembershipSet(size);
        IMembershipSet partial = full.filter(row -> (row % 2) == 0);
        Table myTable = new Table(columns, partial, null);
        assertEquals(myTable.toString(), "Table[2x50]");
        ITable smallTable = myTable.compress();
        assertEquals(smallTable.toString(), "Table[2x50]");
    }

    @Test
    public void corrTableTest() {
        int size = 1000;
        int range = 10;
        int numCols = 4;
        SmallTable table = TestTables.getCorrelatedCols(size, numCols, range);
        assertEquals(table.toString(), "Table[4x1000]");
    }

    @Test
    public void tableTest1() {
        final int size = 100;
        final int numCols =2;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(IntArrayGenerator.getMissingIntArray("A", size, 5));
        columns.add(generateDoubleArray(size, 100));
        final FullMembershipSet full = new FullMembershipSet(size);
        final IMembershipSet partial = full.filter(row -> (row % 2) == 0);
        final Table myTable = new Table(columns, partial, null);
        assertEquals(myTable.toString(), "Table[2x50]");
        HashSubSchema filter = new HashSubSchema();
        filter.add(columns.get(1).getDescription().name);
        ITable smallTable = myTable.compress(filter, partial);
        assertEquals(smallTable.toString(), "Table[1x50]");
    }
}
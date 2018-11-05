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

package org.hillview.test.table;

import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DateListColumn;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.hillview.utils.IntArrayGenerator;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hillview.test.table.DoubleArrayTest.generateDoubleArray;

public class TableTest extends BaseTest {
    @Test
    public void getTableTest() {
        final SmallTable leftTable = TestTables.getIntTable(100, 2);
        Assert.assertNotNull(leftTable);
    }

    @Test
    public void renameTest0() {
        final SmallTable table = TestTables.getIntTable(100, 2);
        HashMap<String, String> renameMap = new
                HashMap<String, String>();
        renameMap.put("Column0", "First");
        ITable tbl = table.renameColumns(renameMap);
        IColumn col = tbl.getLoadedColumn("First");
        Assert.assertNotNull(col);
        col = tbl.getLoadedColumn("Column1");
        Assert.assertNotNull(col);
    }

    @Test
    public void renameTest1() {
        ITable t = TestTables.testListTable();
        HashMap<String, String> renameMap = new
                HashMap<String, String>();
        renameMap.put("Name", "Firstname");
        t = t.renameColumns(renameMap);
        IColumn col = t.getLoadedColumn("Firstname");
        Assert.assertNotNull(col);
        col = t.getLoadedColumn("Age");
        Assert.assertNotNull(col);
    }

    @Test
    public void columnCompressTest() {
        final int size = 100;
        final IntArrayColumn col = IntArrayGenerator.getMissingIntArray("X", size, 5);
        final FullMembershipSet FM = new FullMembershipSet(size);
        final IMembershipSet PMD = FM.filter(row -> (row % 2) == 0);
        final IColumn smallCol = col.compress(PMD);
        Assert.assertNotNull(smallCol);
    }

    @Test
    public void dateColumnCompressTest() {
        final DateListColumn dates = new DateListColumn(
                new ColumnDescription("Test", ContentsKind.Date));
        Instant first = LocalDate.of(2010, Month.APRIL, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
        dates.append(first);
        dates.append(first.plusSeconds(10));
        IMembershipSet set = new FullMembershipSet(2);
        IColumn col = dates.compress(set);
        double d = col.getDouble(0);
        Instant i = col.getDate(0);
        Assert.assertNotNull(i);
        Assert.assertEquals(Converters.toDouble(i), d, .1);
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
        Table myTable = new Table(columns, partial, null, null);
        Assert.assertEquals(myTable.toString(), "Table[2x50]");
        ITable smallTable = myTable.compress();
        Assert.assertEquals(smallTable.toString(), "Table[2x50]");
    }

    @Test
    public void corrTableTest() {
        int size = 1000;
        int range = 10;
        int numCols = 4;
        SmallTable table = TestTables.getCorrelatedCols(size, numCols, range);
        Assert.assertEquals(table.toString(), "Table[4x1000]");
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
        final Table myTable = new Table(columns, partial, null, null);
        Assert.assertEquals(myTable.toString(), "Table[2x50]");
        String[] keep = new String[] { columns.get(1).getDescription().name };
        ITable smallTable = myTable.compress(keep, partial);
        Assert.assertEquals(smallTable.toString(), "Table[1x50]");
    }
}

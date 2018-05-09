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

import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.IMap;
import org.hillview.maps.ConvertColumnMap;
import org.hillview.sketches.DistinctStrings;
import org.hillview.sketches.DistinctStringsSketch;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.utils.JsonList;
import org.hillview.utils.TestTables;
import org.hillview.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ToCatMapTest extends BaseTest {
    static Table tableWithStringColumn() {
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.String);
        ColumnDescription c1 = new ColumnDescription("Age", ContentsKind.Integer);
        StringArrayColumn sac = new StringArrayColumn(c0,
                new String[] { "Mike", "John", "Tom", "Bill", "Bill", "Smith", "Donald", "Bruce",
                        "Bob", "Frank", "Richard", "Steve", "Dave", "Mike", "Ed" });
        IntArrayColumn iac = new IntArrayColumn(c1, new int[] { 20, 30, 10, 10, 20, 30, 20, 30, 10,
                40, 40, 20, 10, 50, 60 });
        return new Table(Arrays.asList(sac, iac), null, null);
    }

    @Test
    public void testToCatMap() {
        ITable table = ToCatMapTest.tableWithStringColumn();
        TestUtils.printTable("Table before conversion:", table);
        IMap<ITable, ITable> map = new ConvertColumnMap(
                "Name", "Name Categorical", ContentsKind.Category, 1);
        ITable result = map.apply(table);
        TestUtils.printTable("Table after conversion:", result);

        ColumnAndConverterDescription nameCat =
                new ColumnAndConverterDescription("Name Categorical");
        ColumnAndConverterDescription name =
                new ColumnAndConverterDescription("Name");
        ColumnAndConverter nameCC = result.getLoadedColumn(name);
        ColumnAndConverter nameCatCC = result.getLoadedColumn(nameCat);

        Assert.assertSame(nameCatCC.column.getDescription().kind, ContentsKind.Category);
        IRowIterator rowIt = result.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            Assert.assertEquals(nameCC.getString(row), nameCatCC.getString(row));
            row = rowIt.getNextRow();
        }
    }

    @Test
    public void testToCatMapBig() {
        ITable table = ToCatMapTest.tableWithStringColumn();
        IDataSet<ITable> bigTable = TestTables.makeParallel(table, 3);
        IMap<ITable, ITable> map = new ConvertColumnMap(
                "Name", "Name Categorical", ContentsKind.Category, 1);

        IDataSet<ITable> result = bigTable.blockingMap(map);

        DistinctStringsSketch uss1 = new DistinctStringsSketch(100, new String[]{"Name"});
        DistinctStringsSketch uss2 = new DistinctStringsSketch(100, new String[]{"Name Categorical"});
        JsonList<DistinctStrings> ds1 = result.blockingSketch(uss1);
        JsonList<DistinctStrings> ds2 = result.blockingSketch(uss2);
        Set<String> strings1 = new HashSet<String>();
        for (String s : ds1.get(0).getStrings()) {
            strings1.add(s);
        }
        Set<String> strings2 = new HashSet<String>();
        for (String s : ds2.get(0).getStrings()) {
            strings2.add(s);
        }

        Assert.assertEquals(strings1, strings2);
    }
}

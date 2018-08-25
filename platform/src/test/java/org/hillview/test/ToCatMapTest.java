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

import org.hillview.dataset.api.IMap;
import org.hillview.maps.ConvertColumnMap;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

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
        IColumn nameCC = result.getLoadedColumn("Name");
        IColumn nameCatCC = result.getLoadedColumn("Name Categorical");

        Assert.assertSame(nameCatCC.getDescription().kind, ContentsKind.Category);
        IRowIterator rowIt = result.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            Assert.assertEquals(nameCC.getString(row), nameCatCC.getString(row));
            row = rowIt.getNextRow();
        }
    }
}

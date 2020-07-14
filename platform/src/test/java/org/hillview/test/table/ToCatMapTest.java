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

import org.hillview.dataset.api.IMap;
import org.hillview.maps.ConvertColumnMap;
import org.hillview.table.api.*;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class ToCatMapTest extends BaseTest {
    @Test
    public void testToCatMap() {
        ITable table = TestTables.testRepTable();
        //TestUtils.printTable("Table before conversion:", table);
        ConvertColumnMap.Info info = new ConvertColumnMap.Info(
                "Name", "Name Categorical", 1, ContentsKind.String);
        IMap<ITable, ITable> map = new ConvertColumnMap(info);
        ITable result = map.apply(table);
        Assert.assertNotNull(result);
        //TestUtils.printTable("Table after conversion:", result);
        IColumn nameCC = result.getLoadedColumn("Name");
        IColumn nameCatCC = result.getLoadedColumn("Name Categorical");

        Assert.assertSame(nameCatCC.getDescription().kind, ContentsKind.String);
        IRowIterator rowIt = result.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            Assert.assertEquals(nameCC.getString(row), nameCatCC.getString(row));
            row = rowIt.getNextRow();
        }
    }
}

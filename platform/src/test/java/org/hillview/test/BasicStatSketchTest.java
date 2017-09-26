/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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
import org.hillview.sketches.*;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.utils.TestTables;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

public class BasicStatSketchTest {
    @Test
    public void StatSketchTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        final BasicColStatSketch mySketch = new BasicColStatSketch(
                new ColumnNameAndConverter(myTable.getSchema().getColumnNames().iterator().next()),
                0 , 0.1);
        BasicColStats result = mySketch.create(myTable);
        Assert.assertEquals(result.getPresentCount(), 100);
    }

    @Test
    public void StatSketchTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();

        IDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final BasicColStats result = all.blockingSketch(
                new BasicColStatSketch(new ColumnNameAndConverter(colName, null)));
        final BasicColStatSketch mySketch = new BasicColStatSketch(
                new ColumnNameAndConverter(
                        bigTable.getSchema().getColumnNames().iterator().next()));
        BasicColStats result1 = mySketch.create(bigTable);
        Assert.assertEquals(result.getMoment(1), result1.getMoment(1), 0.001);
    }
}

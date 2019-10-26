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

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.SampleQuantileSketch;
import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.sketches.results.SampleList;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;

public class TableDataSetTest extends BaseTest {
    @Test
    public void localDataSetTest() {
        int numCols = 3;
        int size = 1000, resolution = 20;
        SmallTable randTable = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.getSchema().getDescription(colName), true));
        }
        SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size, 0);
        LocalDataSet<ITable> ld = new LocalDataSet<ITable>(randTable);
        SampleList sl = ld.blockingSketch(sqSketch);
        Assert.assertNotNull(sl);
        IndexComparator comp = cso.getIndexComparator(sl.table);
        for (int i = 0; i < (sl.table.getNumOfRows() - 1); i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
        //System.out.println(ql);
    }

    @Test
    public void parallelDataSetTest() {
        int numCols = 3;
        int size = 1000, resolution = 20;
        SmallTable randTable1 = TestTables.getIntTable(size, numCols);
        SmallTable randTable2 = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable1.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable1.getSchema().getDescription(colName), true));
        }
        LocalDataSet<ITable> ld1 = new LocalDataSet<ITable>(randTable1);
        LocalDataSet<ITable> ld2 = new LocalDataSet<ITable>(randTable2);
        ArrayList<IDataSet<ITable>> elements = new ArrayList<IDataSet<ITable>>(2);
        elements.add(ld1);
        elements.add(ld2);
        ParallelDataSet<ITable> par = new ParallelDataSet<ITable>(elements);
        SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size, 0);
        SampleList sl = par.blockingSketch(sqSketch);
        Assert.assertNotNull(sl);
        IndexComparator comp = cso.getIndexComparator(sl.table);
        for (int i = 0; i < (sl.table.getNumOfRows() - 1); i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
    }
}

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

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.api.ITable;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.api.IndexComparator;
import org.junit.Test;

import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;

public class TableDataSetTest {
    @Test
    public void localDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final SmallTable randTable = TableTest.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.getSchema().getDescription(colName), true));
        }
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final LocalDataSet<ITable> ld = new LocalDataSet<ITable>(randTable);
        final QuantileList ql = ld.blockingSketch(qSketch);
        IndexComparator comp = cso.getComparator(ql.quantile);
        for (int i = 0; i < (ql.getQuantileSize() - 1); i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
        //System.out.println(ql);
    }

    @Test
    public void parallelDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final SmallTable randTable1 = TableTest.getIntTable(size, numCols);
        final SmallTable randTable2 = TableTest.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable1.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable1.getSchema().getDescription(colName), true));
        }

        final LocalDataSet<ITable> ld1 = new LocalDataSet<ITable>(randTable1);
        final LocalDataSet<ITable> ld2 = new LocalDataSet<ITable>(randTable2);
        final ArrayList<IDataSet<ITable>> elems = new ArrayList<IDataSet<ITable>>(2);
        elems.add(ld1);
        elems.add(ld2);
        final ParallelDataSet<ITable> par = new ParallelDataSet<ITable>(elems);
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final QuantileList r = par.blockingSketch(qSketch);
        IndexComparator comp = cso.getComparator(r.quantile);
        for (int i = 0; i < (r.getQuantileSize() - 1); i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
        //System.out.println(r);
    }
}
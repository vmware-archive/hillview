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

package org.hillview.sketch;

import com.google.common.net.HostAndPort;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.remoting.HillviewServer;
import org.hillview.sketches.*;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.utils.TestTables;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;

public class TableDataSetTest {
    @Test
    public void localDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final SmallTable randTable = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.getSchema().getDescription(colName), true));
        }
        final SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size);
        final LocalDataSet<ITable> ld = new LocalDataSet<ITable>(randTable);
        final SampleList sl = ld.blockingSketch(sqSketch);
        IndexComparator comp = cso.getComparator(sl.table);
        for (int i = 0; i < (sl.table.getNumOfRows()- 1); i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
        //System.out.println(ql);
    }

    @Test
    public void parallelDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final SmallTable randTable1 = TestTables.getIntTable(size, numCols);
        final SmallTable randTable2 = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable1.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable1.getSchema().getDescription(colName), true));
        }
        final LocalDataSet<ITable> ld1 = new LocalDataSet<ITable>(randTable1);
        final LocalDataSet<ITable> ld2 = new LocalDataSet<ITable>(randTable2);
        final ArrayList<IDataSet<ITable>> elements = new ArrayList<IDataSet<ITable>>(2);
        elements.add(ld1);
        elements.add(ld2);
        final ParallelDataSet<ITable> par = new ParallelDataSet<ITable>(elements);
        final SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size);
        final SampleList sl = par.blockingSketch(sqSketch);
        IndexComparator comp = cso.getComparator(sl.table);
        for (int i = 0; i < (sl.table.getNumOfRows() - 1); i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
    }

    @Test
    public void remoteDataSetTest() throws IOException {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final SmallTable randTable = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.getSchema().getDescription(colName), true));
        }
        final SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size);
        final HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1234);
        final HillviewServer server1 = new HillviewServer(h1, new LocalDataSet<ITable>(randTable));
        try {
            final RemoteDataSet<ITable> rds1 = new RemoteDataSet<>(h1);
            final SampleList sl = rds1.blockingSketch(sqSketch);
            IndexComparator comp = cso.getComparator(sl.table);
            for (int i = 0; i < (sl.table.getNumOfRows() - 1); i++)
                assertTrue(comp.compare(i, i + 1) <= 0);
        } finally {
            server1.shutdown();
        }
    }
}
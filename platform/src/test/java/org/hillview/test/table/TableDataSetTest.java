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

import net.jcip.annotations.NotThreadSafe;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.sketches.*;
import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.sketches.results.NextKList;
import org.hillview.sketches.results.SampleList;
import org.hillview.table.ColumnDescription;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.test.BaseTest;
import org.hillview.utils.HostAndPort;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;

@NotThreadSafe
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

    @Test
    public void remoteDataSetTest() throws IOException {
        int numCols = 3;
        int size = 1000, resolution = 20;
        SmallTable randTable = TestTables.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.getSchema().getDescription(colName), true));
        }
        SampleQuantileSketch sqSketch = new SampleQuantileSketch(cso, resolution, size, 0);
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1234);
        HillviewServer server1 = new HillviewServer(h1, new LocalDataSet<ITable>(randTable));
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            SampleList sl = rds1.blockingSketch(sqSketch);
            Assert.assertNotNull(sl);
            IndexComparator comp = cso.getIndexComparator(sl.table);
            for (int i = 0; i < (sl.table.getNumOfRows() - 1); i++)
                assertTrue(comp.compare(i, i + 1) <= 0);
        } finally {
            server1.shutdown();
        }
    }

    @Test
    public void remoteDataSetTest1() throws IOException {
        RecordOrder cso = new RecordOrder();
        cso.append(new ColumnSortOrientation(new ColumnDescription("Column0", ContentsKind.Integer), true));
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1235);
        ArrayList<IDataSet<ITable>> one = new ArrayList<IDataSet<ITable>>();
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(TestTables.getIntTable(20, 2));
        one.add(local);
        IDataSet<ITable> small = new ParallelDataSet<ITable>(one);
        HillviewServer server1 = new HillviewServer(h1, small);
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            NextKSketch sketch = new NextKSketch(cso, null, null, 10);
            NextKList sl = rds1.blockingSketch(sketch);
            Assert.assertNotNull(sl);
            Assert.assertEquals("Table[1x10]", sl.rows.toString());
        } finally {
            server1.shutdown();
        }
    }

    @Test
    public void remoteDataSetTest2() throws IOException {
        RecordOrder cso = new RecordOrder();
        cso.append(new ColumnSortOrientation(new ColumnDescription("Name", ContentsKind.String), true));
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1236);
        ArrayList<IDataSet<ITable>> empty = new ArrayList<IDataSet<ITable>>();
        IDataSet<ITable> small = new ParallelDataSet<ITable>(empty);
        HillviewServer server1 = new HillviewServer(h1, small);
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            NextKSketch sketch = new NextKSketch(cso, null, null, 10);
            NextKList sl = rds1.blockingSketch(sketch);
            Assert.assertNotNull(sl);
            Assert.assertEquals("Table[1x0]", sl.rows.toString());
        } finally {
            server1.shutdown();
        }
    }

    @Test
    public void remoteDataSetTest3() throws IOException {
        RecordOrder cso = new RecordOrder();
        cso.append(new ColumnSortOrientation(new ColumnDescription("Name", ContentsKind.String), true));
        HostAndPort h1 = HostAndPort.fromParts("127.0.0.1", 1237);
        HostAndPort h2 = HostAndPort.fromParts("127.0.0.1", 1238);

        ArrayList<IDataSet<ITable>> one = new ArrayList<IDataSet<ITable>>();
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(TestTables.testTable());
        one.add(local);
        IDataSet<ITable> small = new ParallelDataSet<ITable>(one);
        HillviewServer server1 = new HillviewServer(h1, small);

        ArrayList<IDataSet<ITable>> none = new ArrayList<IDataSet<ITable>>();
        IDataSet<ITable> empty = new ParallelDataSet<ITable>(none);
        HillviewServer server2 = new HillviewServer(h2, empty);
        try {
            RemoteDataSet<ITable> rds1 = new RemoteDataSet<ITable>(h1);
            RemoteDataSet<ITable> rds2 = new RemoteDataSet<ITable>(h2);
            ArrayList<IDataSet<ITable>> two = new ArrayList<IDataSet<ITable>>();
            two.add(rds1);
            two.add(rds2);
            ParallelDataSet<ITable> top = new ParallelDataSet<ITable>(two);
            NextKSketch sketch = new NextKSketch(cso, null, null, 10);
            NextKList sl = top.blockingSketch(sketch);
            Assert.assertNotNull(sl);
            Assert.assertEquals("Table[1x10]", sl.rows.toString());
        } finally {
            server1.shutdown();
            server2.shutdown();
        }
    }
}

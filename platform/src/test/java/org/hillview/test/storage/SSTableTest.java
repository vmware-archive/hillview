/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.test.storage;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.hillview.storage.CassandraConnectionInfo;
import org.hillview.storage.CassandraDatabase;
import org.hillview.storage.CassandraSSTableLoader;
import org.hillview.storage.CassandraDatabase.CassTable;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("FieldCanBeLocal")
public class SSTableTest extends BaseTest {
    /* The directory where cassandra is installed (check bin/install-cassandra.sh) */
    private final String cassandraRootDir = System.getenv("HOME") + "/cassandra";
    private final String ssTableDir = "../data/sstable/";
    private final String ssTablePath = "../data/sstable/md-2-big-Data.db";

    public CassandraConnectionInfo getConnectionInfo() {
        CassandraConnectionInfo conn = new CassandraConnectionInfo();
        conn.cassandraRootDir = cassandraRootDir;
        conn.host = "localhost";
        conn.jmxPort = 7199;
        conn.port = 9042;
        conn.database = "cassdb";
        conn.table = "flights";
        conn.databaseKind = "Cassandra";
        conn.user = "";
        conn.password = "";
        conn.lazyLoading = true;
        return conn;
    }

    @Test
    public void testSSTableComplimentaryFiles() {
        File directoryPath = new File(this.ssTableDir);
        if (!directoryPath.exists())
            return;
        String[] contents = directoryPath.list();
        Assert.assertNotNull(contents);
        int counter = 0;
        for (String content : contents) {
            if (content.startsWith("md-"))
                counter++;
        }
        Assert.assertEquals(8, counter);
    }

    @Test
    public void testReadingSSTable() {
        boolean lazyLoading = false;
        File directoryPath = new File(this.ssTableDir);
        if (!directoryPath.exists())
            return;
        CassandraSSTableLoader ssTableLoader = CassandraSSTableLoader.getTestTableLoader(this.ssTablePath, lazyLoading);
        ITable table = ssTableLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[4x15]", table.toString());
    }

    @Test
    public void testRowCount() {
        File directoryPath = new File(this.ssTableDir);
        if (!directoryPath.exists())
            return;
        boolean lazyLoading = false;
        CassandraSSTableLoader ssTableLoader = CassandraSSTableLoader
                .getTestTableLoader(this.ssTablePath, lazyLoading);
        int rowCount = ssTableLoader.getRowCount();
        Assert.assertEquals(15, rowCount);
    }

    @Test
    public void testLazyLoading() {
        File directoryPath = new File(this.ssTableDir);
        if (!directoryPath.exists())
            return;
        boolean lazyLoading = true;
        CassandraSSTableLoader ssTableLoader = CassandraSSTableLoader
                .getTestTableLoader(this.ssTablePath, lazyLoading);
        ITable table = ssTableLoader.load();
        Assert.assertNotNull(table);

        IColumn col = table.getLoadedColumn("name");
        String firstName = col.getString(0);
        Assert.assertEquals("susi", firstName);

        List<String> colNames = Arrays.asList("salary","address");
        List<IColumn> listCols = table.getLoadedColumns(colNames);
        String address = listCols.get(1).getString(3);
        int salary = listCols.get(0).getInt(3);

        List<String> colNames2 = Arrays.asList("name","salary","phone");
        List<IColumn> listCols2 = table.getLoadedColumns(colNames2);
        String phone = listCols2.get(2).getString(14);
        String name = listCols2.get(0).getString(14);

        Assert.assertEquals(2, listCols.size());
        Assert.assertEquals("Hyderabad", address);
        Assert.assertEquals(40000, salary);
        Assert.assertEquals("9848022338", phone);
        Assert.assertEquals("ram", name);
    }

    /** Checking whether the given cassandra root is exist and contains bin directory */
    @Test
    public void testCassandraDirectory() {
        // If this test failed, then cassandraRootDir must be updated to a valid path
        CassandraConnectionInfo conn = this.getConnectionInfo();
        Path cassandraRootDir = Paths.get(conn.cassandraRootDir);
        Path cassandraBinaryDir = Paths.get(conn.cassandraRootDir + "/bin");
        File rootDir = cassandraRootDir.toFile();
        if (rootDir.exists()) {
            Assert.assertTrue(rootDir.isDirectory());
            Assert.assertTrue(cassandraBinaryDir.toFile().isDirectory());
        }
    }

    /** Make sure the local cassandra instance has the necessary table that will be tested */
    @Test
    public void testStoredTableInfo() {
        CassandraConnectionInfo conn = null;
        CassandraDatabase db = null;
        try {
            // Connecting to Cassandra node and get some data
            conn = this.getConnectionInfo();
            db = new CassandraDatabase(conn);
        } catch (Exception e) {
            // this will fail if local Cassandra is not running, but we don't want to fail the test.
            this.ignoringException("Failed connecting to local cassandra", e);
            return;
        }
        List<CassTable> storedTable = db.getStoredTableInfo();
        Assert.assertTrue(storedTable.toString().contains("cassdb: [ test counter flights users]"));
    }

    /** Shows the interaction between CassandraDatabase.java and CassandraSSTableLoader.java */
    @Test
    public void testCassandraDatabase() {
        CassandraConnectionInfo conn = null;
        CassandraDatabase db = null;
        try {
            // Connecting to Cassandra node and get some data
            conn = this.getConnectionInfo();
            db = new CassandraDatabase(conn);
        } catch (Exception e) {
            // this will fail if local Cassandra is not running, but we don't want to fail the test.
            this.ignoringException("Failed connecting to local cassandra", e);
            return;
        }
        List<String> arrSSTablePath = db.getSSTablePath();
        Assert.assertEquals(1, arrSSTablePath.size());
        String ssTablePath = db.getSSTablePath().get(0);
        Assert.assertTrue(ssTablePath.endsWith(CassandraDatabase.ssTableFileMarker));
        // Reading the SSTable of flights data
        CassandraSSTableLoader ssTableLoader = CassandraSSTableLoader
                .getTestTableLoader(ssTablePath, conn.lazyLoading);
        ITable table = ssTableLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[15x100]", table.toString());
        IColumn col = table.getLoadedColumn("origincityname");
        String origincityname = col.getString(0);
        Assert.assertEquals("Dallas/Fort Worth, TX", origincityname);
    }

    @Test
    public void testCassandraTypeConversion() {
        CassandraConnectionInfo conn = null;
        CassandraDatabase db = null;
        try {
            // Connecting to Cassandra node and get some data
            conn = this.getConnectionInfo();
            conn.table = "test";
            db = new CassandraDatabase(conn);
        } catch (Exception e) {
            // this will fail if local Cassandra is not running, but we don't want to fail the test.
            this.ignoringException("Failed connecting to local cassandra", e);
            return;
        }
        List<String> arrSSTablePath = db.getSSTablePath();
        Assert.assertEquals(1, arrSSTablePath.size());
        String ssTablePath = db.getSSTablePath().get(0);
        Assert.assertTrue(ssTablePath.endsWith(CassandraDatabase.ssTableFileMarker));
        CassandraSSTableLoader ssTableLoader = CassandraSSTableLoader
                .getTestTableLoader(ssTablePath, conn.lazyLoading);
        ITable table = ssTableLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[20x2]", table.toString());
        List<IColumn> listCols = table.getLoadedColumns(ssTableLoader.getSchema().getColumnNames());

        int nullIdx = listCols.get(0).isMissing(0) ? 0 : 1; // this row contains mostly null values
        int nonNullIdx = nullIdx == 0 ? 1 : 0;
        // ascii type
        Assert.assertEquals("35", listCols.get(0).getString(nonNullIdx));
        Assert.assertEquals(true, listCols.get(0).isMissing(nullIdx));
        // bigint type
        Assert.assertEquals(true, listCols.get(1).isMissing(nullIdx));
        // blob type
        Assert.assertEquals("0x61646231346662653037366636623934343434633636306533366134303031353166323666633666",
                listCols.get(2).getString(nonNullIdx));
        // boolean type
        Assert.assertEquals("true", listCols.get(3).getString(nonNullIdx));
        // date type
        Assert.assertEquals("2020-07-14T00:00:00Z", listCols.get(4).getDate(nonNullIdx).toString());
        // decimal type
        Assert.assertEquals(3.7875, listCols.get(5).getDouble(nonNullIdx), 1);
        // double type
        Assert.assertEquals(true, listCols.get(6).isMissing(nullIdx));
        Assert.assertEquals("6.714592679340089E9", Double.toString(listCols.get(6).getDouble(nonNullIdx)));
        // duration type
        Assert.assertEquals(Converters.toDuration(131405000L), listCols.get(7).getDuration(nonNullIdx));
        // float type
        Assert.assertEquals(true, listCols.get(8).isMissing(nullIdx));
        Assert.assertEquals(3.1475300788879395, listCols.get(8).getDouble(nonNullIdx), 1);
        // inet type
        Assert.assertEquals("/127.0.0.1", listCols.get(9).getString(nonNullIdx));
        // int type
        Assert.assertEquals(true, listCols.get(10).isMissing(nonNullIdx));
        Assert.assertEquals(true, listCols.get(10).isMissing(nullIdx));
        // name: string type 
        Assert.assertEquals("Mr. Test", listCols.get(11).getString(nonNullIdx));
        // salary: integer type
        Assert.assertEquals(45000, listCols.get(12).getInt(nonNullIdx));
        Assert.assertEquals(true, listCols.get(12).isMissing(nullIdx));
        // smallint type
        Assert.assertEquals(1, listCols.get(13).getInt(nonNullIdx));
        // text type
        Assert.assertEquals(null, listCols.get(14).getString(nonNullIdx));
        // time type
        Assert.assertEquals(Instant.ofEpochMilli(946755023123L), listCols.get(15).getDate(nonNullIdx));
        // timestamp type
        Assert.assertEquals("2017-05-05T20:00:00Z", listCols.get(16).getDate(nonNullIdx).toString());
        // timeuuid type
        Assert.assertEquals("50554d6e-29bb-11e5-b345-feff819cdc9f", listCols.get(17).getString(nonNullIdx));
        // tinyint type
        Assert.assertEquals(2, listCols.get(18).getInt(nonNullIdx));
        // varint type
        Assert.assertEquals(10, listCols.get(19).getDouble(nonNullIdx), 1);
    }

    @Test
    public void testCassandraCounterTable() {
        CassandraConnectionInfo conn = null;
        CassandraDatabase db = null;
        try {
            // Connecting to Cassandra node and get some data
            conn = this.getConnectionInfo();
            conn.table = "counter";
            db = new CassandraDatabase(conn);
        } catch (Exception e) {
            // this will fail if local Cassandra is not running, but we don't want to fail the test.
            this.ignoringException("Failed connecting to local cassandra", e);
            return;
        }
        List<String> arrSSTablePath = db.getSSTablePath();
        Assert.assertEquals(1, arrSSTablePath.size());
        String ssTablePath = db.getSSTablePath().get(0);
        Assert.assertTrue(ssTablePath.endsWith(CassandraDatabase.ssTableFileMarker));
        CassandraSSTableLoader ssTableLoader = CassandraSSTableLoader
                .getTestTableLoader(ssTablePath, conn.lazyLoading);
        ITable table = ssTableLoader.load();
        Assert.assertNotNull(table);
        IColumn column = table.getLoadedColumn("counter");
        Assert.assertEquals(1, column.getDouble(0), 1);
    }
}
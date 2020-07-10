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
import java.util.Arrays;
import java.util.List;

import org.hillview.storage.CassandraConnectionInfo;
import org.hillview.storage.CassandraDatabase;
import org.hillview.storage.CassandraSSTableLoader;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

public class SSTableTest extends BaseTest{
    /* The directory where cassandra is installed (check bin/install-cassandra.sh) */
    private final String cassandraRootDir = "/tmp/cassandra";
    private final String ssTableDir = "../data/sstable/";
    private final String ssTablePath = "../data/sstable/md-2-big-Data.db";
    private final CassandraConnectionInfo conn;
    CassandraDatabase cassDB;

    /** This test depends on a local data, I will update this in the future. */
    public SSTableTest() {
        this.conn = new CassandraConnectionInfo();
        this.conn.cassandraRootDir = cassandraRootDir;
        this.conn.host = "localhost";
        this.conn.jmxPort = 7199;
        this.conn.port = 9042;
        this.conn.database = "cassdb";
        this.conn.table = "flights";
        this.conn.databaseKind = "Cassandra";
        this.conn.user = "";
        this.conn.password = "";
        this.conn.lazyLoading = true;
        this.cassDB = new CassandraDatabase(this.conn);
    }

    @Test
    public void testSSTableComplimentaryFiles() {
        try {
            File directoryPath = new File(ssTableDir);
            String[] contents = directoryPath.list();
            Assert.assertNotNull(contents);
            int counter = 0;
            for (String content : contents) {
                if (content.startsWith("md-"))
                    counter++;
            }
            Assert.assertEquals(8, counter);
        } catch (Exception e) {
            e.printStackTrace();
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + ssTablePath + ")", e);
        }
    }

    @Test
    public void testReadingSSTable() {
        boolean lazyLoading = false;
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(this.ssTablePath, lazyLoading);
        try {
            ITable table = ssTableLoader.load();

            Assert.assertNotNull(table);
            Assert.assertEquals("Table[4x15]", table.toString());
        } catch (Exception e) {
            e.printStackTrace();
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + this.ssTablePath + ")", e);
        }
    }

    @Test
    public void testRowCount() {
        boolean lazyLoading = false;
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(this.ssTablePath, lazyLoading);
        try {
            long rowCount = ssTableLoader.getNumRows();
            Assert.assertEquals(15, rowCount);
        } catch (Exception e) {
            e.printStackTrace();
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + this.ssTablePath + ")", e);
        }
    }

    @Test
    public void testLazyLoading() {
        boolean lazyLoading = true;
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(this.ssTablePath, lazyLoading);
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + this.ssTablePath + ")", e);
        }
    }

    @Test
    /** Checking whether the given cassandra root is exist and contains bin directory */
    public void TestCassandraDirectory(){
        // If this test failed, then cassandraRootDir must be updated to a valid path
        Path cassandraRootDir = Paths.get(this.conn.cassandraRootDir);
        Path cassandraBinaryDir = Paths.get(this.conn.cassandraRootDir + "/bin");
        Assert.assertEquals(true, cassandraRootDir.toFile().isDirectory());
        Assert.assertEquals(true, cassandraBinaryDir.toFile().isDirectory());
    }

    @Test
    /** Shows the interaction between CassandraDatabase.java and CassandraSSTableLoader.java */
    public void TestCassandraDatabase() {
        try {
            // Connecting to Cassandra node and get some data
                String ssTablePath = this.cassDB.getSSTablePath();
                Assert.assertEquals(true, ssTablePath.endsWith(CassandraDatabase.ssTableFileMarker));
            // Reading the SSTable of flights data
                CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(ssTablePath, this.conn.lazyLoading);

                ITable table = ssTableLoader.load();
                Assert.assertEquals("Table[15x100]", table.toString());

                IColumn col = table.getLoadedColumn("origincityname");
                String origincityname = col.getString(0);
                Assert.assertEquals("Dallas/Fort Worth, TX", origincityname);
        } catch (Exception e) {
            // this will fail if no running Cassandra instance, but we don't want to fail the test.
            this.ignoringException("Failed connecting to local cassandra", e);
        }
    }
}
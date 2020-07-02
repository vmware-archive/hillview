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

package org.hillview.test.storage;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.hillview.storage.CassandraSSTableLoader;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

public class SSTableTest extends BaseTest{
    private String ssTableDir = "../data/sstable/";
    private String ssTablePath = "../data/sstable/md-2-big-Data.db";
        
    @Test
    public void testSSTableComplimentaryFiles() throws Exception {
        try{
            File directoryPath = new File(ssTableDir);
            String contents[] = directoryPath.list();
            int counter = 0;
            for(int i=0; i<contents.length; i++) {
                if(contents[i].startsWith("md-"))
                    counter ++;
            }
            Assert.assertEquals(8, counter);
        } catch (Exception e) {
            System.out.println(e.getStackTrace().toString());
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + ssTablePath + ")", e);
            return;
        }
    }

    @Test
    public void testReadingSSTable() throws Exception {
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(this.ssTablePath, false);
        try{
            ITable table = ssTableLoader.load();
            Assert.assertEquals("Table[4x15]", table.toString());
        } catch (Exception e) {
            System.out.println(e.getStackTrace().toString());
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + this.ssTablePath + ")", e);
            return;
        }
    }

    @Test
    public void testRowCount() throws Exception {
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(this.ssTablePath, false);
        try{
            int rowCount = ssTableLoader.getNumRows();
            Assert.assertEquals(15, rowCount);
        } catch (Exception e) {
            System.out.println(e.getStackTrace().toString());
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + this.ssTablePath + ")", e);
            return;
        }
    }

    @Test
    public void testLazyLoading() throws Exception {
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(this.ssTablePath, true);
        try{
            ITable table = ssTableLoader.load();
            IColumn col = table.getLoadedColumn("name");
            String firstName = col.getString(0);
            Assert.assertEquals("susi", firstName);

            List<String> colNames = Arrays.asList("salary","address");
            List<IColumn> listCols = table.getLoadedColumns(colNames);
            String address = listCols.get(1).getString(3);
            int salary = listCols.get(0).getInt(3);

            Assert.assertEquals(2, listCols.size());
            Assert.assertEquals("Hyderabad", address);
            Assert.assertEquals(40000, salary);
        } catch (Exception e) {
            System.out.println(e.getStackTrace().toString());
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + this.ssTablePath + ")", e);
            return;
        }
    }
}
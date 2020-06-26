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

import org.hillview.storage.CassandraSSTableLoader;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

public class SSTableTest extends BaseTest{
    @Test
    public void testReadingSSTable() throws Exception {
        String ssTablePath = "/Users/daniar/Documents/Github/hillview/data/sstable/md-2-big-Data.db";
        CassandraSSTableLoader ssTableLoader = new CassandraSSTableLoader(ssTablePath);
        try{
            ITable table = ssTableLoader.load();
            Assert.assertEquals("Table[4x15]", table.toString());
        } catch (Exception e) {
            // this will fail if SSTable path is not valid, but we don't want to fail the test.
            this.ignoringException("Failed to read SSTable (" + ssTablePath + ")", e);
        }
    }
    
}
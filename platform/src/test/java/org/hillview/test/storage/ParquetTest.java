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

package org.hillview.test.storage;

import org.hillview.storage.ParquetFileLoader;
import org.hillview.table.Table;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

public class ParquetTest extends BaseTest {
    // Not yet checked-in into the repository
    private static final String path = dataDir + "/parquet/" +
            "part-r-00000-9d5cd245-a2e4-4002-9d58-0efdfb0fb962.gz.parquet";

    @Test
    public void readTest() {
        ITable table;
        try {
            ParquetFileLoader pr = new ParquetFileLoader(path, false);
            table = pr.load();
        } catch (Exception ex) {
            // If the file is not present do not fail the test.
            return;
        }

        Assert.assertNotNull(table);
        Assert.assertEquals("Table[18x4214]", table.toString());
        IColumn first = table.getLoadedColumn("java_version");
        Assert.assertEquals(first.getString(0), "1.8.0_91");
    }

    @Test
    public void lazyReadTest() {
        ITable table;
        try {
            ParquetFileLoader pr = new ParquetFileLoader(path, true);
            table = pr.load();
        } catch (Exception ex) {
            // If the file is not present do not fail the test.
            return;
        }

        Assert.assertNotNull(table);
        Assert.assertEquals("Table[18x4214]", table.toString());
        IColumn first = table.getLoadedColumn("java_version");
        Assert.assertEquals(first.getString(0), "1.8.0_91");
        Table tbl = (Table)table;
        Assert.assertFalse(tbl.getColumns().get(1).isLoaded());
    }
}

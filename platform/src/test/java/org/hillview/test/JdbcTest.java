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

package org.hillview.test;

import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.storage.JdbcDatabase;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Various tests for reading data from databases through JDBC.
 * The MySQL tests assume that the MySQL test_db from
 * https://github.com/datacharmer/test_db has
 * been loaded into MySQL.
 */
public class JdbcTest extends BaseTest {
    @Nullable
    private ITable getTable(JdbcConnectionInformation conn) {
        Assert.assertNotNull(conn.table);
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
            ITable table = db.readTable();
            db.disconnect();
            return table;
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            ignoringException("Cannot connect to database", e);
            return null;
        }
    }

    @Test
    public void testMysqlConnection() {
        JdbcConnectionInformation conn = new JdbcConnectionInformation();
        conn.databaseKind = "mysql";
        conn.port = 3306;
        conn.host = "localhost";
        conn.database = "employees";
        conn.table = "salaries";
        conn.user = "user";
        conn.password = "password";
        ITable table = this.getTable(conn);
        if (table != null)
            Assert.assertEquals("Table[4x2844047]", table.toString());
    }

    @Test
    public void testImpalaConnection() {
        JdbcConnectionInformation conn = new JdbcConnectionInformation();
        conn.databaseKind = "impala";
        conn.host = "localhost";
        conn.port = 21050;
        conn.database = "employees";
        conn.table = "salaries";
        conn.user = "user";
        conn.password = "password";
        this.getTable(conn);
    }

    @Test
    public void testMysqlLazy() {
        JdbcConnectionInformation conn = new JdbcConnectionInformation();
        conn.databaseKind = "mysql";
        conn.port = 3306;
        conn.host = "localhost";
        conn.database = "employees";
        conn.table = "salaries";
        conn.user = "user";
        conn.password = "password";
        conn.lazyLoading = true;
        ITable table = this.getTable(conn);
        if (table != null) {
            Assert.assertEquals("Table[4x2844047]", table.toString());
            IColumn col = table.getLoadedColumn("salary");
            int firstSalary = col.getInt(0);
            Assert.assertEquals(60117, firstSalary);

            IColumn emp = table.getLoadedColumn("emp_no");
            int empNo = emp.getInt(0);
            Assert.assertEquals(10001, empNo);
        }
    }
}

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

import org.hillview.storage.CsvFileWriter;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.storage.JdbcDatabase;
import org.hillview.utils.Converters;
import org.junit.Test;
import rx.Observer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Various tests for reading data from databases through JDBC.
 */
public class JdbcTest extends BaseTest {
    @Nullable
    ITable getTable(JdbcConnectionInformation conn, int rowLimit) {
        Converters.checkNull(conn.table);
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
            ResultSet dbTable = db.getTable(conn.table, rowLimit);
            ITable table = JdbcDatabase.getTable(dbTable);
            db.disconnect();
            return table;
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            System.out.println("Cannot connect to database");
            e.printStackTrace();
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
        ITable table = this.getTable(conn, 10);
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
        ITable table = this.getTable(conn, -1);
    }

    // Downloads an impala table and splits it into pieces.
    public void downloadSplitTable() {
        JdbcConnectionInformation conn = new JdbcConnectionInformation();
        conn.databaseKind = "impala";
        conn.host = "vmware.com";
        conn.port = 21050;
        conn.database = "employees";
        conn.table = "salaries";
        conn.user = "user";
        conn.password = "password";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
            ResultSet dbTable = db.getTable(conn.table, -1);
            Observer<ITable> obs = new Observer<ITable>() {
                int index = 0;

                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                }

                @Override
                public void onNext(ITable tbl) {
                    System.out.println("Received table " + index);
                    String file = conn.table + index;
                    if (index == 0)
                        tbl.getSchema().writeToJsonFile(Paths.get(conn.table + ".schema"));
                    CsvFileWriter fw = new CsvFileWriter(file + ".csv");
                    fw.setCompress(true);
                    try {
                        fw.writeTable(tbl);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    index++;
                }
            };
            JdbcDatabase.getTables(dbTable, 100000, obs);
            db.disconnect();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

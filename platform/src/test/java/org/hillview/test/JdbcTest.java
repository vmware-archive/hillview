/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

import org.hillview.table.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.table.JdbcDatabase;
import org.junit.Test;

import java.sql.ResultSet;

public class JdbcTest {
    @Test
    public void testJdbc() {
        JdbcConnectionInformation conn =
                new JdbcConnectionInformation("localhost", "employees", "mbudiu", "password");
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
            ResultSet salaries = db.getTable("salaries");
            ITable table = JdbcDatabase.getTable(salaries);
            db.disconnect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            System.out.println("Cannot connect to database");
            e.printStackTrace();
        }
    }
}

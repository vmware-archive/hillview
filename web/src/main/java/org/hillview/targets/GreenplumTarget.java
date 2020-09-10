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

package org.hillview.targets;

import org.hillview.HillviewComputation;
import org.hillview.HillviewRpc;
import org.hillview.RpcRequest;
import org.hillview.RpcRequestContext;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.storage.jdbc.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.utils.JsonString;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class GreenplumTarget extends SimpleDBTarget {
    public GreenplumTarget(JdbcConnectionInformation conn, HillviewComputation c, String dir) {
        super(conn, c, dir);
    }

    @HillviewRpc
    public void dumpTable(RpcRequest request, RpcRequestContext context) throws SQLException {
        String tmpTableName = UUID.randomUUID().toString();
        String query = "CREATE WRITABLE EXTERNAL TABLE " +
                tmpTableName + "(LIKE " + this.table + ") LOCATION('gpfdist:///tmp/" +
                tmpTableName + "') FORMAT 'CSV'";

        Statement st = this.database.createStatement();
        st.executeUpdate(query);
        query = "INSERT INTO " + tmpTableName + " SELECT * FROM " + this.table;
        st = this.database.createStatement();
        st.executeUpdate(query);

        PrecomputedSketch<ITable, JsonString> sk = new PrecomputedSketch<ITable, JsonString>(
                new JsonString(tmpTableName));
        this.runCompleteSketch(this.table, sk, request, context);
    }
}

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

import org.hillview.*;
import org.hillview.storage.jdbc.JdbcConnectionInformation;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonInString;

import java.sql.SQLException;

/**
 * This target is the first interface to a Greenplum database.
 * It inherits some operations from SimpleDBTarget, in particular,
 * getSummary.
 */
public class GreenplumStubTarget extends SimpleDBTarget {
    static final String filePrefix = "file";  // Should match the prefix in the dump script

    public GreenplumStubTarget(JdbcConnectionInformation conn, HillviewComputation c, String dir) {
        super(conn, c, dir);
    }

    @HillviewRpc
    public void initializeTable(RpcRequest request, RpcRequestContext context) throws SQLException {
        String tmpTableName = request.parseArgs(String.class);
        Converters.checkNull(this.schema);
        /*
        In this scheme, which does not seem to work, we only dump the first column now,
        and we load the other ones later, when needed.

        List<String> col = Utilities.list(this.schema.getColumnNames().get(0));
        GreenplumTarget.writeColumns(col, this.database, this.schema, tmpTableName);
         */

        // Create an external table that will be written into
        String tableName = this.jdbc.table;
        String query = "CREATE WRITABLE EXTERNAL WEB TABLE " +
                tmpTableName + " (LIKE " + tableName + ") EXECUTE '" +
                Configuration.instance.getGreenplumDumpScript() + " " +
                Configuration.instance.getGreenplumDumpDirectory() + "/" + tmpTableName +
                "' FORMAT 'CSV'";
        database.executeUpdate(query);
        // This triggers the dumping of the data on the workers
        query = "INSERT INTO " + tmpTableName + " SELECT * FROM " + tableName;
        database.executeUpdate(query);
        // Cleanup: remove temporary table and view
        query = "DROP EXTERNAL TABLE " + tmpTableName;
        database.executeUpdate(query);
        this.database.disconnect();
        this.returnResult(JsonInString.makeJsonString(
                Configuration.instance.getGreenplumDumpDirectory() + "/" + tmpTableName + "/" + filePrefix + "*"),
            request, context);
    }
}

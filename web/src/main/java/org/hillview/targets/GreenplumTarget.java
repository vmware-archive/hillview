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
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.storage.jdbc.JdbcConnectionInformation;
import org.hillview.table.api.ITable;
import org.hillview.utils.JsonInString;

import java.sql.SQLException;

@SuppressWarnings("SqlNoDataSourceInspection")
public class GreenplumTarget extends SimpleDBTarget {
    static final String filePrefix = "file";  // Should match the prefix in the dump script

    /*
        This is the expected contents of the dump-greenplum.sh script:
#!/bin/sh
DIR=$1
PREFIX="file"
mkdir -p ${DIR} || exit 1
echo "$(</dev/stdin)" >${DIR}/${PREFIX}${GP_SEGMENT_ID}
     */

    public GreenplumTarget(JdbcConnectionInformation conn, HillviewComputation c, String dir) {
        super(conn, c, dir);
    }

    @HillviewRpc
    public void dumpTable(RpcRequest request, RpcRequestContext context) throws SQLException {
        String tmpTableName = request.parseArgs(String.class);
        String dumpScriptName = RpcObjectManager.instance.properties.getProperty(
                "greenplumDumpScript", "/home/gpadmin/hillview/dump-greenplum.sh");
        String dumpDirectory = RpcObjectManager.instance.properties.getProperty(
                "greenplumDumpDirectory", "/tmp");

        // This creates a virtual table that will write its partitions
        // in files named like ${dumpDirectory}/${tmpTableName}/${filePrefix}Number
        String tableName = this.jdbc.table;
        String query = "CREATE WRITABLE EXTERNAL WEB TABLE " +
                tmpTableName + " (LIKE " + tableName + ") EXECUTE '" +
                dumpScriptName + " " + dumpDirectory + "/" + tmpTableName + "' FORMAT 'CSV'";

        this.database.executeUpdate(query);
        query = "INSERT INTO " + tmpTableName + " SELECT * FROM " + tableName;
        this.database.executeUpdate(query);

        PrecomputedSketch<ITable, JsonInString> sk = new PrecomputedSketch<ITable, JsonInString>(
                JsonInString.makeJsonString(dumpDirectory + "/" + tmpTableName + "/" + filePrefix + "*"));
        this.runCompleteSketch(this.table, sk, request, context);
    }
}

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
import org.hillview.dataset.api.ControlMessage;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.LoadCsvColumnsSketch;
import org.hillview.storage.jdbc.JdbcDatabase;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * This class interfaces with a Greenplum database.  It is very much like a TableTarget, except
 * that for each operation is checks to see first whether the columns that will be operated on
 * have been loaded, and if not loads them explicitly.
 */
public class GreenplumTarget extends TableTarget {
    JdbcDatabase database;

    protected HashSet<String> columnsLoaded;
    protected String tmpTableName;
    protected final Schema schema;

    public GreenplumTarget(IDataSet<ITable> table, HillviewComputation c,
                           @Nullable String metadataDir,
                           String tmpTableName, JdbcDatabase db, Schema schema) {
        super(table, c, metadataDir);
        this.database = db;
        this.schema = schema;
        this.columnsLoaded = new HashSet<String>();
        this.tmpTableName = tmpTableName;
        this.columnsLoaded.add(this.schema.getColumnNames().get(0));
        try {
            this.database.connect();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Given a list of columns, find the ones which have not been loaded yet.
     * @param columns  Columns to check.
     * @return         A set of columns that need to be loaded.
     */
    protected List<String> columnsToLoad(Iterable<String> columns) {
        List<String> result = new ArrayList<>();
        for (String c: columns) {
            if (!this.columnsLoaded.contains(c))
                result.add(c);
        }
        return result;
    }

    public static void writeColumns(Iterable<String> columns, JdbcDatabase database,
                                    Schema schema,
                                    String tmpTableName) throws SQLException {
        String tableName = database.connInfo.table;
        StringBuilder cols = new StringBuilder();
        boolean first = true;
        for (String c: columns) {
            if (!first)
                cols.append(", ");
            first = false;
            ColumnDescription desc = schema.getDescription(c);
            String type = JdbcDatabase.sqlType(desc.kind);
            cols.append(c).append(" ").append(type);
        }
        // Create an external table that will be written into
        String query = "CREATE WRITABLE EXTERNAL WEB TABLE " +
                tmpTableName + " (" + cols.toString() + ") EXECUTE '" +
                Configuration.instance.getGreenplumDumpScript() + " " +
                Configuration.instance.getGreenplumDumpDirectory() + "/" + tmpTableName +
                "' FORMAT 'CSV'";
        database.executeUpdate(query);
        // This triggers the dumping of the data on the workers
        query = "INSERT INTO " + tmpTableName + " SELECT " + String.join(", ", columns) + " FROM " + tableName;
        database.executeUpdate(query);
        // Cleanup: remove temporary table and view
        query = "DROP EXTERNAL TABLE " + tmpTableName;
        database.executeUpdate(query);
    }

    protected void writeColumns(Iterable<String> columns) throws SQLException {
        writeColumns(columns, this.database, this.schema, this.tmpTableName);
    }

    protected void loadWrittenColumns(List<String> columns) {
        // Ask remote workers to parse their local files
        HashSet<String> set = new HashSet<>(columns);
        Schema toLoad = this.schema.project(set::contains);
        LoadCsvColumnsSketch sketch = new LoadCsvColumnsSketch(toLoad);
        ControlMessage.StatusList sl = this.table.blockingSketch(sketch);
        for (ControlMessage.Status s: Converters.checkNull(sl))
            if (s.isError())
                throw new RuntimeException(s.exception);
        this.columnsLoaded.addAll(columns);
    }

    synchronized protected void ensureColumns(Iterable<String> columns) {
        try {
            List<String> cols = this.columnsToLoad(columns);
            if (cols.isEmpty())
                return;
            this.writeColumns(cols);
            this.loadWrittenColumns(cols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @HillviewRpc
    public void getNextK(RpcRequest request, RpcRequestContext context) {
        NextKArgs nextKArgs = request.parseArgs(NextKArgs.class);
        Schema schema = nextKArgs.order.toSchema();
        this.ensureColumns(schema.getColumnNames());
        super.getNextK(request, context);
    }
}

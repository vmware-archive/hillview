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

package org.hillview.storage;

import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import rx.Observer;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Conversions between JDBC information and ITable objects.
 */
public class JdbcDatabase {
    private final JdbcConnection conn;
    @Nullable
    private Connection connection;

    public JdbcDatabase(final JdbcConnectionInformation connInfo) {
        this.conn = JdbcConnection.create(connInfo);
        this.connection = null;
    }

    public void connect() throws SQLException {
        String url = this.conn.getURL();
        HillviewLogger.instance.info("Database server url", "{0}", url);
        if (Utilities.isNullOrEmpty(this.conn.info.password)) {
            this.connection = DriverManager.getConnection(url);
        } else {
            if (Utilities.isNullOrEmpty(this.conn.info.user))
                this.conn.info.user = System.getProperty("user.name");
            this.connection = DriverManager.getConnection(
                    url, this.conn.info.user, this.conn.info.password);
        }
    }

    public void disconnect() throws SQLException {
        if (this.connection == null)
            return;
        this.connection.close();
        this.connection = null;
    }

    /**
     * Get the data in a table.
     * @param table     Table to get data for.
     * @param rowCount  Maximum number of rows.  If negative, bring all rows.
     */
    public ResultSet getTable(String table, int rowCount) {
        String query = this.conn.getQueryToReadTable(table, rowCount);
        try {
            return this.getQueryResult(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the data produced by a query.
     * @param query     Query to execute.
     */
    public ResultSet getQueryData(String query) {
        try {
            return this.getQueryResult(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet getQueryResult(String query) throws SQLException {
        Statement st = Converters.checkNull(this.connection).createStatement();
        return st.executeQuery(query);
    }

    static ColumnDescription getDescription(ResultSetMetaData meta, int colIndex)
            throws SQLException {
        colIndex = colIndex + 1;

        String name = meta.getColumnLabel(colIndex);
        ContentsKind kind;

        int colType = meta.getColumnType(colIndex);
        switch (colType) {
            case Types.BOOLEAN:
            case Types.BIT:
                kind = ContentsKind.Category;
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                kind = ContentsKind.Integer;
                break;
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                kind = ContentsKind.Double;
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                kind = ContentsKind.String;
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                kind = ContentsKind.Date;
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            case Types.DATALINK:
            case Types.ROWID:
            case Types.NCLOB:
            case Types.REF_CURSOR:
            default:
                throw new RuntimeException("Unhandled column type " + colType);
        }
        return new ColumnDescription(name, kind);
    }

    static Schema getSchema(ResultSet data) {
        try {
            ResultSetMetaData meta = data.getMetaData();
            Schema result = new Schema();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                result.append(cd);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert a resultSet to a sequence of tables.
     * @param data     Result set obtained from JDBC.
     * @param maxRows  Maximum rows to use in a table.  If 0 there is no limit.
     * @param observer Observer that receives the tables produced.
     */
    public static void getTables(ResultSet data, int maxRows, Observer<ITable> observer) {
        try {
            ResultSetMetaData meta = data.getMetaData();
            List<IAppendableColumn> cols = null;

            int rowsRead = 0;
            while (data.next()) {
                rowsRead++;
                if (cols == null) {
                    // Allocate a new table
                    cols = new ArrayList<IAppendableColumn>();
                    for (int i = 0; i < meta.getColumnCount(); i++) {
                        ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                        BaseListColumn col = BaseListColumn.create(cd);
                        cols.add(col);
                    }
                }

                if (rowsRead % 50000 == 0)
                    System.out.print(".");
                for (int i = 0; i < cols.size(); i++) {
                    int colIndex = i + 1;
                    IAppendableColumn col = cols.get(i);
                    int colType = meta.getColumnType(colIndex);
                    switch (colType) {
                        case Types.BOOLEAN:
                        case Types.BIT:
                            boolean b = data.getBoolean(colIndex);
                            if (data.wasNull())
                                col.appendMissing();
                            else
                                col.append(b ? "true" : "false");
                            break;
                        case Types.TINYINT:
                        case Types.SMALLINT:
                        case Types.INTEGER:
                            int integer = data.getInt(colIndex);
                            if (data.wasNull())
                                col.appendMissing();
                            else
                                col.append(integer);
                            break;
                        case Types.BIGINT:
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            double d = data.getDouble(colIndex);
                            if (data.wasNull())
                                col.appendMissing();
                            else
                                col.append(d);
                            break;
                        case Types.CHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.SQLXML:
                            String s = data.getString(colIndex);
                            col.append(s);
                            break;
                        case Types.DATE:
                        case Types.TIME:
                        case Types.TIMESTAMP:
                        case Types.TIME_WITH_TIMEZONE:
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                            Timestamp ts = data.getTimestamp(colIndex);
                            if (ts == null) {
                                col.appendMissing();
                            } else {
                                Instant instant = ts.toInstant();
                                col.append(instant);
                            }
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                        case Types.NULL:
                        case Types.OTHER:
                        case Types.JAVA_OBJECT:
                        case Types.DISTINCT:
                        case Types.STRUCT:
                        case Types.ARRAY:
                        case Types.BLOB:
                        case Types.CLOB:
                        case Types.REF:
                        case Types.DATALINK:
                        case Types.ROWID:
                        case Types.NCLOB:
                        case Types.REF_CURSOR:
                        default:
                            throw new RuntimeException("Unhandled column type " + colType);
                    }
                }

                if (maxRows != 0 && rowsRead % maxRows == 0) {
                    ITable table = new Table(cols);
                    observer.onNext(table);
                    // Force a new allocation for columns.
                    cols = null;
                }
            }

            // Create one last table
            if (cols != null) {
                ITable table = new Table(cols);
                observer.onNext(table);
            }
        } catch (SQLException e) {
            observer.onError(e);
        }
    }

    public static ITable getTable(ResultSet rs) {
        final ITable[] result = {null};
        final Throwable[] th = {null};
        Observer<ITable> obs = new Observer<ITable>() {
            @Override
            public void onCompleted() { }

            @Override
            public void onError(Throwable throwable) {
                th[0] = throwable;
            }

            @Override
            public void onNext(ITable table) {
                result[0] = table;
            }
        };

        getTables(rs, 0, obs);
        if (th[0] != null)
            throw new RuntimeException(new Exception(th[0]));
        return result[0];
    }
}

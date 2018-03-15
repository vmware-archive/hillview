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
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.table.columns.LazyColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import rx.Observer;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.Instant;
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

    public ITable readTable() {
        try {
            Converters.checkNull(this.conn.info.table);
            if (this.conn.info.lazyLoading) {
                String query = this.conn.getQueryToReadSize(this.conn.info.table);
                ResultSet rs = this.getQueryResult(query);
                if (!rs.next())
                    throw new RuntimeException("Could not retrieve table size for " + this.conn.info.table);
                int rowCount = rs.getInt(1);

                IColumnLoader loader = new JdbcLoader(this.conn.info);

                ResultSetMetaData meta = this.getSchema();
                LazyColumn[] cols = new LazyColumn[meta.getColumnCount()];
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                    LazyColumn col = new LazyColumn(cd, rowCount, loader);
                    cols[i] = col;
                }
                Table result = new Table(cols);
                result.setColumnLoader(loader);
                return result;
            } else {
                ResultSet rs = this.getDataInTable(-1);
                IAppendableColumn[] columns = JdbcDatabase.convertResultSet(rs);
                return new Table(columns);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSetMetaData getSchema() {
        try {
            ResultSet rs = this.getDataInTable(0);
            return rs.getMetaData();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This class knows how to read a set of columns from a database.
     */
    static class JdbcLoader implements IColumnLoader {
        private final JdbcConnectionInformation connInfo;

        public JdbcLoader(final JdbcConnectionInformation connInfo) {
            this.connInfo = connInfo;
        }

        @Override
        public IColumn[] loadColumns(List<String> names) {
            try {
                JdbcDatabase db = new JdbcDatabase(this.connInfo);
                db.connect();
                String cols = String.join(",", names);
                String query = "SELECT " + cols + " FROM " + this.connInfo.table;
                ResultSet rs = db.getQueryResult(query);
                IAppendableColumn[] columns = JdbcDatabase.convertResultSet(rs);
                db.disconnect();
                return columns;
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Get the data in the JDBC database table.
     * @param rowCount  Maximum number of rows.  If negative, bring all rows.
     */
    ResultSet getDataInTable(int rowCount) {
        Converters.checkNull(this.conn.info.table);
        String query = this.conn.getQueryToReadTable(this.conn.info.table, rowCount);
        return this.getQueryResult(query);
    }

    ResultSet getQueryResult(String query) {
        try {
            HillviewLogger.instance.info("Executing SQL query", "{0}", query);
            Statement st = Converters.checkNull(this.connection).createStatement();
            return st.executeQuery(query);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
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

    private static void appendNext(IAppendableColumn[] cols, ResultSetMetaData meta, ResultSet data)
            throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            int colIndex = i + 1;
            IAppendableColumn col = cols[i];
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
    }

    static IAppendableColumn[] createColumns(ResultSetMetaData meta) throws SQLException {
        IAppendableColumn[] cols = new IAppendableColumn[meta.getColumnCount()];
        for (int i = 0; i < meta.getColumnCount(); i++) {
            ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
            BaseListColumn col = BaseListColumn.create(cd);
            cols[i] = col;
        }
        return cols;
    }

    /**
     * Convert a resultSet to a sequence of arrays of columns.
     * @param data     Result set obtained from JDBC.
     * @param maxRows  Maximum rows to use in a table.  If 0 there is no limit.
     * @param observer Observer that receives the tables produced.
     */
    static void convertResultSet(ResultSet data, int maxRows,
                                 Observer<IAppendableColumn[]> observer) {
        try {
            ResultSetMetaData meta = data.getMetaData();
            IAppendableColumn[] cols = createColumns(meta);

            int rowsRead = 0;
            int rowsInLastBatch = 0;
            while (data.next()) {
                rowsRead++;
                rowsInLastBatch++;
                appendNext(cols, meta, data);
                if (rowsRead % 50000 == 0)
                    System.out.print(".");
                if (maxRows != 0 && rowsRead % maxRows == 0) {
                    observer.onNext(cols);
                    // Force a new allocation for columns.
                    cols = createColumns(meta);
                    rowsInLastBatch = 0;
                }
            }

            if (rowsInLastBatch > 0 || rowsRead == 0)
                observer.onNext(cols);
        } catch (SQLException e) {
            observer.onError(e);
        }
    }

    static IAppendableColumn[] convertResultSet(ResultSet rs) {
        final IAppendableColumn[][] result = { { null } };
        final Throwable[] th = { null };
        Observer<IAppendableColumn[]> obs = new Observer<IAppendableColumn[]>() {
            @Override
            public void onCompleted() { }

            @Override
            public void onError(Throwable throwable) {
                th[0] = throwable;
            }

            @Override
            public void onNext(IAppendableColumn[] cols) { result[0] = cols; }
        };

        convertResultSet(rs, 0, obs);
        if (th[0] != null)
            throw new RuntimeException(new Exception(th[0]));
        return result[0];
    }
}

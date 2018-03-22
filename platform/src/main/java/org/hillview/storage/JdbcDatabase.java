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
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Linq;
import org.hillview.utils.Utilities;

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
                List<ColumnDescription> cds = new ArrayList<ColumnDescription>(
                        meta.getColumnCount());
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                    cds.add(cd);
                }
                return Table.createLazyTable(cds, rowCount, this.conn.info.table, loader);
            } else {
                ResultSet rs = this.getDataInTable(-1);
                List<IAppendableColumn> columns = JdbcDatabase.convertResultSet(rs);
                return new Table(columns, this.conn.info.table, null);
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
        public List<IColumn> loadColumns(List<String> names) {
            try {
                JdbcDatabase db = new JdbcDatabase(this.connInfo);
                db.connect();
                String cols = String.join(",", names);
                String query = "SELECT " + cols + " FROM " + this.connInfo.table;
                ResultSet rs = db.getQueryResult(query);
                List<IAppendableColumn> columns = JdbcDatabase.convertResultSet(rs);
                db.disconnect();
                return Linq.map(columns, c -> c);
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

    public ITable getQueryData(String query) {
        ResultSet rs = this.getQueryResult(query);
        List<IAppendableColumn> columns = JdbcDatabase.convertResultSet(rs);
        return new Table(columns, null, null);
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

    private static void appendNext(List<IAppendableColumn> cols,
                                   ResultSetMetaData meta, ResultSet data)
            throws SQLException {
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
    }

    static List<IAppendableColumn> createColumns(ResultSetMetaData meta) throws SQLException {
        List<IAppendableColumn> cols = new ArrayList<IAppendableColumn>(meta.getColumnCount());
        for (int i = 0; i < meta.getColumnCount(); i++) {
            ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
            BaseListColumn col = BaseListColumn.create(cd);
            cols.add(col);
        }
        return cols;
    }

    static List<IAppendableColumn> convertResultSet(ResultSet data) {
        try {
            ResultSetMetaData meta = data.getMetaData();
            List<IAppendableColumn> cols = createColumns(meta);

            int rowsRead = 0;
            while (data.next()) {
                rowsRead++;
                appendNext(cols, meta, data);
                if (rowsRead % 50000 == 0)
                    System.out.print(".");
            }
            return cols;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

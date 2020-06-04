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

import org.hillview.sketches.results.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.*;

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

    public int getRowCount(@Nullable ColumnLimits columnLimits) {
        try {
            assert this.conn.info.table != null;
            String query = this.conn.getQueryToReadSize(columnLimits);
            ResultSet rs = this.getQueryResult(query);
            if (!rs.next())
                throw new RuntimeException("Could not retrieve table size for " + this.conn.info.table);
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
            assert this.conn.info.table != null;
            if (this.conn.info.lazyLoading) {
                int rowCount = this.getRowCount(null);
                IColumnLoader loader = new JdbcLoader(this.conn.info);
                ResultSetMetaData meta = this.getTableSchema();
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

    private ResultSetMetaData getTableSchema() {
        try {
            ResultSet rs = this.getDataInTable(0);
            return rs.getMetaData();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Schema getSchema() {
        try {
            Schema result = new Schema();
            ResultSetMetaData meta = this.getTableSchema();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                result.append(cd);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int distinctCount(String columnName, @Nullable ColumnLimits columnLimits) {
        try {
            assert this.conn.info.table != null;
            String query = this.conn.getQueryForDistinctCount(columnName, columnLimits);
            ResultSet rs = this.getQueryResult(query);
            if (!rs.next())
                throw new RuntimeException("Could not retrieve column for " + this.conn.info.table);
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Find rows with top frequencies in the specified columns.
     * @param schema  Columns to look at.
     * @param maxRows Maximum number of rows expected.
     * @param columnLimits  Filtering rules for database.
     * @return        A SmallTable that contains the frequent elements.  The
     *                last column has the count of each row.
     */
    public SmallTable topFreq(Schema schema, int maxRows,
                              @Nullable ColumnLimits columnLimits) {
        assert this.conn.info.table != null;
        String query = this.conn.getQueryToComputeFreqValues(schema, maxRows, columnLimits);
        ResultSet rs = this.getQueryResult(query);
        List<IAppendableColumn> columns = JdbcDatabase.convertResultSet(rs);
        return new SmallTable(columns);
    }

    /**
     * Computes a histogram on the specified column with the specified buckets.
     * @param cd       Description of column to histogram.
     * @param columnLimits   Limits on column values.
     * @param buckets  Bucket description
     * @param rowCount Number of rows in the database.
     * @return         The histogram of the data.
     */
    public Histogram histogram(ColumnDescription cd, IHistogramBuckets buckets,
                               @Nullable ColumnLimits columnLimits,
                               @Nullable ColumnQuantization quantization,
                               int rowCount) {
        String query = this.conn.getQueryForHistogram(cd, columnLimits, buckets, quantization);
        ResultSet rs = this.getQueryResult(query);
        List<IAppendableColumn> cols = JdbcDatabase.convertResultSet(rs);
        assert cols.size() == 2;
        IColumn bucketNr = cols.get(0);
        IColumn bucketSize = cols.get(1);
        boolean isDouble = bucketNr.getDescription().kind == ContentsKind.Double;
        int bucketCount = buckets.getBucketCount();
        long[] data = new long[bucketCount];
        long nonNulls = 0;
        for (int i = 0; i < bucketNr.sizeInRows(); i++) {
            int index = isDouble ? Converters.toInt(bucketNr.getDouble(i)) : bucketNr.getInt(i);
            // In SQL the last bucket boundary is not inclusive, so sometimes
            // we may get an extra bucket.  The semantics in Hillview is to fold
            // that into the penultimate bucket.
            if (index == bucketCount)
                index--;
            long count = Converters.toLong(bucketSize.getDouble(i));
            data[index] += count;
            nonNulls += count;
        }
        long nulls = rowCount - nonNulls;
        return new Histogram(data, nulls);
    }

    public JsonGroups<JsonGroups<Count>>
    histogram2D(ColumnDescription cd0, ColumnDescription cd1,
                IHistogramBuckets buckets0, IHistogramBuckets buckets1,
                @Nullable ColumnLimits columnLimits,
                @Nullable ColumnQuantization quantization0,
                @Nullable ColumnQuantization quantization1) {
        // TODO: this does not currently compute nulls
        String query = this.conn.getQueryForHeatmap(cd0, cd1,
                columnLimits,
                buckets0, buckets1,
                quantization0, quantization1);
        ResultSet rs = this.getQueryResult(query);
        List<IAppendableColumn> cols = JdbcDatabase.convertResultSet(rs);
        assert cols.size() == 2;
        IColumn bucketNr = cols.get(0);
        IColumn bucketSize = cols.get(1);
        boolean isDouble = bucketNr.getDescription().kind == ContentsKind.Double;
        int b0 = buckets0.getBucketCount();
        int b1 = buckets1.getBucketCount();

        long[][] buckets = new long[b0][b1];
        for (int i = 0; i < bucketNr.sizeInRows(); i++) {
            int index = isDouble ? Converters.toInt(bucketNr.getDouble(i)) : bucketNr.getInt(i);
            long count = Converters.toLong(bucketSize.getDouble(i));
            int x0 = index >> 16;
            // In SQL the last bucket boundary is not inclusive, so sometimes
            // we may get an extra bucket.  The semantics in Hillview is to fold
            // that into the penultimate bucket.
            if (x0 == b0)
                x0--;
            int y0 = index & 0xFFFF;
            if (y0 == b1)
                y0--;
            buckets[x0][y0] += count;
        }
        Count z = new Count(0);
        return new JsonGroups<JsonGroups<Count>>(
                b0, i -> i < 0 ? new JsonGroups<Count>(b1, z) :
                new JsonGroups<Count>(b1, j -> j < 0 ? z : new Count(buckets[i][j])));
    }

    /**
     * Computes the range of the data in a column.
     * @param cd  Description of the column.
     * @param limits  Limits on the data to read.
     */
    public DataRange numericDataRange(ColumnDescription cd, @Nullable ColumnLimits limits) {
        String query = this.conn.getQueryForNumericRange(cd, null, limits);
        ResultSet rs = this.getQueryResult(query);
        List<IAppendableColumn> cols = JdbcDatabase.convertResultSet(rs);
        SmallTable table = new SmallTable(cols);
        assert table.getNumOfRows() == 1;
        RowSnapshot row = new RowSnapshot(table, 0);
        DataRange range = new DataRange();
        if (cd.kind == ContentsKind.Double) {
            range.min = row.getDouble("min");
            range.max = row.getDouble("max");
        } else if (cd.kind == ContentsKind.Integer) {
            range.min = row.getInt("min");
            range.max = row.getInt("max");
        } else if (cd.kind == ContentsKind.Date) {
            Instant min = row.getDate("min");
            Instant max = row.getDate("max");
            if (min != null)
                range.min = Converters.toDouble(min);
            if (max != null)
                range.max = Converters.toDouble(max);
        }
        range.presentCount = Converters.toLong(row.getDouble("nonnulls"));
        range.missingCount = Converters.toLong(row.getDouble("total")) - range.presentCount;
        return range;
    }

    public StringQuantiles stringBuckets(ColumnDescription cd, int stringsToSample,
                                         @Nullable ColumnLimits columnLimits) {
        @Nullable String max = null;
        JsonList<String> boundaries = new JsonList<String>();
        long presentCount, missingCount;
        int rows;
        {
            // Compute boundaries
            String query = this.conn.getQueryForDistinct(cd, columnLimits);
            ResultSet rs = this.getQueryResult(query);
            List<IAppendableColumn> cols = JdbcDatabase.convertResultSet(rs);
            assert cols.size() == 1;
            IAppendableColumn col = cols.get(0);
            rows = col.sizeInRows();
            if (rows <= stringsToSample) {
                for (int i = 0; i < rows; i++) {
                    max = col.getString(i);
                    boundaries.add(max);
                }
            } else {
                for (int i = 0; i < stringsToSample; i++)
                    boundaries.add(col.getString(i * rows / stringsToSample));
                max = col.getString(col.sizeInRows() - 1);
            }
        }
        {
            // Compute presentCount and missingCount
            String query = this.conn.getQueryForCounts(cd, null, columnLimits);
            ResultSet rs = this.getQueryResult(query);
            List<IAppendableColumn> cols = JdbcDatabase.convertResultSet(rs);
            SmallTable table = new SmallTable(cols);
            assert table.getNumOfRows() == 1;
            RowSnapshot row = new RowSnapshot(table, 0);
            presentCount = Converters.toLong(row.getDouble("nonnulls"));
            missingCount = Converters.toLong(row.getDouble("total")) - presentCount;
        }

        return new StringQuantiles(
                boundaries, max, rows <= stringsToSample, presentCount, missingCount);
    }

    /**
     * This class knows how to read a set of columns from a database.
     */
    static class JdbcLoader implements IColumnLoader {
        private final JdbcConnectionInformation connInfo;

        JdbcLoader(final JdbcConnectionInformation connInfo) {
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
    private ResultSet getDataInTable(int rowCount) {
        assert this.conn.info.table != null;
        String query = this.conn.getQueryToReadTable(rowCount);
        return this.getQueryResult(query);
    }

    private ResultSet getQueryResult(String query) {
        try {
            // System.out.println(query);
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

    private static ColumnDescription getDescription(ResultSetMetaData meta, int colIndex)
            throws SQLException {
        colIndex = colIndex + 1;
        String name = meta.getColumnLabel(colIndex);
        ContentsKind kind;
        int colType = meta.getColumnType(colIndex);
        switch (colType) {
            case Types.BOOLEAN:
            case Types.BIT:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                kind = ContentsKind.String;
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

    private static List<IAppendableColumn> createColumns(ResultSetMetaData meta) throws SQLException {
        List<IAppendableColumn> cols = new ArrayList<IAppendableColumn>(meta.getColumnCount());
        for (int i = 0; i < meta.getColumnCount(); i++) {
            ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
            IAppendableColumn col = BaseListColumn.create(cd);
            cols.add(col);
        }
        return cols;
    }

    private static List<IAppendableColumn> convertResultSet(ResultSet data) {
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


/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.test.storage;

import org.hillview.sketches.results.*;
import org.hillview.storage.ColumnLimits;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.storage.JdbcDatabase;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.table.filters.RangeFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.IGroup;
import org.hillview.utils.JsonGroups;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * Most of theses tests assume that the MySQL test database from
 * https://github.com/datacharmer/test_db has been installed and
 * a user exists with name "user" and password "password".
 */
public class MysqlTest extends JdbcTest {
    /**
     * Returns a connection information suitable for accessing
     * the table "employees" in the "test_db" mysql database.
     */
    private JdbcConnectionInformation mySqlTestDbConnection() {
        JdbcConnectionInformation conn = new JdbcConnectionInformation();
        conn.databaseKind = "mysql";
        conn.port = 3306;
        conn.host = "localhost";
        conn.database = "employees";
        conn.table = "salaries";
        conn.user = "user";
        conn.password = "password";
        return conn;
    }

    @Test
    public void testMysqlConnection() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        ITable table = this.getTable(conn);
        if (table != null)
            Assert.assertEquals("Table[4x2844047]", table.toString());
    }

    @Test
    public void testMysqlLazy() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.lazyLoading = true;
        ITable table = this.getTable(conn);
        if (table != null) {
            Assert.assertEquals("Table[4x2844047]", table.toString());
            IColumn col = table.getLoadedColumn("salary");
            int firstSalary = col.getInt(0);
            Assert.assertEquals(60117, firstSalary);

            IColumn emp = table.getLoadedColumn("emp_no");
            int empNo = emp.getInt(0);
            Assert.assertEquals(10001, empNo);
        }
    }

    @Test
    public void testMysqlRowCount() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        int rows = db.getRowCount(null);
        db.disconnect();
        Assert.assertEquals(2844047, rows);
    }

    @Test
    public void testMysqlDistinct() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        int distinct = db.distinctCount("salary", null);
        db.disconnect();
        Assert.assertEquals(85814, distinct);
    }

    @Test
    public void testMysqlTopK() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        Schema schema = new Schema();
        schema.append(new ColumnDescription("salary", ContentsKind.Double));
        SmallTable tbl = db.topFreq(schema, 10000, null);
        db.disconnect();
        Assert.assertEquals(1, tbl.getNumOfRows());
        //noinspection MismatchedQueryAndUpdateOfCollection
        RowSnapshot row = new RowSnapshot(tbl, 0);
        String col = row.getColumnNames().get(1);
        Assert.assertEquals(95373, (int)row.getDouble(col));
        Assert.assertEquals(40000, row.getInt("salary"));
    }

    @Test
    public void testMysqlRange() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DataRange range = db.numericDataRange(
                new ColumnDescription("salary", ContentsKind.Integer), null);
        Assert.assertNotNull(range);
        Assert.assertEquals(38623.0, range.min, .1);
        Assert.assertEquals(158220.0, range.max, .1);
        Assert.assertEquals(2844047, range.presentCount);
        Assert.assertEquals(0, range.missingCount);
        db.disconnect();
    }

    @Test
    public void testMysqlRangeLimits() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        ColumnDescription cd = new ColumnDescription("salary", ContentsKind.Integer);
        ColumnLimits limits = new ColumnLimits();
        RangeFilterDescription filter = new RangeFilterDescription();
        filter.cd = cd;
        filter.min = 0;
        filter.max = 300000;
        limits.put(filter);
        DataRange range = db.numericDataRange(cd, limits);
        Assert.assertNotNull(range);
        Assert.assertEquals(38623.0, range.min, .1);
        Assert.assertEquals(158220.0, range.max, .1);
        Assert.assertEquals(2844047, range.presentCount);
        Assert.assertEquals(0, range.missingCount);

        filter.max = 100000;
        limits.intersect(filter);
        range = db.numericDataRange(cd, limits);
        Assert.assertNotNull(range);
        Assert.assertEquals(38623.0, range.min, .1);
        Assert.assertEquals(100000.0, range.max, .1);
        Assert.assertEquals(2749351, range.presentCount);
        Assert.assertEquals(0, range.missingCount);
        db.disconnect();
    }

    @Test
    public void testMysqlDateRange() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DataRange range = db.numericDataRange(
                new ColumnDescription("hire_date", ContentsKind.LocalDate), null);
        Assert.assertNotNull(range);
        LocalDateTime first = parseOneLocalDate("1985/01/01");
        LocalDateTime last = parseOneLocalDate("2000/01/28");
        Assert.assertEquals(first, Converters.toLocalDate(range.min));
        Assert.assertEquals(last, Converters.toLocalDate(range.max));
        Assert.assertEquals(300024, range.presentCount);
        Assert.assertEquals(0, range.missingCount);
        db.disconnect();
    }

    @Test
    public void testMysqlDateHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        LocalDateTime first = parseOneLocalDate("1952/02/01");
        LocalDateTime last = parseOneLocalDate("1965/02/01");
        DoubleHistogramBuckets buckets = new DoubleHistogramBuckets("birth_date",
                Converters.toDouble(first), Converters.toDouble(last), 10);
        JsonGroups<Count> histogram = db.histogram(
                new ColumnDescription("birth_date", ContentsKind.LocalDate), buckets,
                null, null, 300024);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(10, histogram.size());
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(300024, total);
        db.disconnect();
    }

    @Test
    public void testMysqlQuantizedDateHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        LocalDateTime first = parseOneLocalDate("1952/02/01");
        LocalDateTime last = parseOneLocalDate("1965/02/01");
        DoubleHistogramBuckets buckets = new DoubleHistogramBuckets("birth_date",
                Converters.toDouble(first), Converters.toDouble(last), 10);
        DoubleColumnQuantization quantization = new DoubleColumnQuantization("birth_date",
                86400, Converters.toDouble(first), Converters.toDouble(last));
        JsonGroups<Count> histogram = db.histogram(
                new ColumnDescription("birth_date", ContentsKind.LocalDate),
                buckets, null, quantization, 300024);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(10, histogram.size());
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(300024, total);
        db.disconnect();
    }

    @Test
    public void testMysqlQuantizedFilteredDateHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        LocalDateTime first = parseOneLocalDate("1955/01/01");
        LocalDateTime last = parseOneLocalDate("1965/02/01");
        DoubleHistogramBuckets buckets = new DoubleHistogramBuckets("birth_date",
                Converters.toDouble(first), Converters.toDouble(last), 10);
        LocalDateTime firstQ = parseOneLocalDate("1952/02/01");
        LocalDateTime lastQ = parseOneLocalDate("1965/02/01");
        DoubleColumnQuantization quantization = new DoubleColumnQuantization("birth_date",
                86400, Converters.toDouble(firstQ), Converters.toDouble(lastQ));
        JsonGroups<Count> histogram = db.histogram(
                new ColumnDescription("birth_date", ContentsKind.LocalDate),
                buckets, null, quantization, 232730);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(10, histogram.size());
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(232730, total);
        db.disconnect();
    }

    @Test
    public void testMysqlStringRange() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        StringQuantiles range = db.stringBuckets(
                new ColumnDescription("first_name", ContentsKind.String), 10, null);
        Assert.assertNotNull(range);
        Assert.assertEquals(10, range.stringQuantiles.size());
        Assert.assertFalse(range.allStringsKnown);
        Assert.assertEquals(300024, range.presentCount);
        Assert.assertEquals(0, range.missingCount);
        String previous = range.stringQuantiles.get(0);
        for (int i = 1; i < range.stringQuantiles.size(); i++) {
            String current = range.stringQuantiles.get(i);
            Assert.assertTrue(previous.compareTo(current) < 0);
            previous = current;
        }
        db.disconnect();
    }

    @Test
    public void testMysqlNumericHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DoubleHistogramBuckets buckets = new DoubleHistogramBuckets("salary", 0, 200000, 8);
        IGroup<Count> histogram = db.histogram(
                new ColumnDescription("salary", ContentsKind.Integer), buckets, null, null, 2844047);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(8, histogram.size());
        Assert.assertEquals(0, histogram.getMissing().count);
        Assert.assertEquals(0, histogram.getBucket(0).count);
        Assert.assertEquals(0, histogram.getBucket(7).count);
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(2844047, total);
        db.disconnect();
    }

    @Test
    public void testMysqlHeatmap() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DoubleHistogramBuckets buckets0 = new DoubleHistogramBuckets("salary", 0, 200000, 8);
        DoubleHistogramBuckets buckets1 = new DoubleHistogramBuckets("emp_no", 0, 500000, 4);
        JsonGroups<JsonGroups<Count>> heatmap = db.histogram2D(
                new ColumnDescription("salary", ContentsKind.Integer),
                new ColumnDescription("emp_no", ContentsKind.Integer),
                buckets0, buckets1, null, null, null);
        Assert.assertNotNull(heatmap);
        int xBucketCount = heatmap.perBucket.size();
        int yBucketCount = heatmap.perBucket.get(0).perBucket.size();
        Assert.assertEquals(8, xBucketCount);
        Assert.assertEquals(4, yBucketCount);
        long total = 0;
        for (int i = 0; i < xBucketCount; i++)
            for (int j = 0; j < yBucketCount; j++)
                total += heatmap.perBucket.get(i).perBucket.get(j).count;
        Assert.assertEquals(2844047, total);
        db.disconnect();
    }

    @Test
    public void testMysqlHeatmapSelection() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DoubleHistogramBuckets buckets0 = new DoubleHistogramBuckets("salary", 0, 200000, 8);
        DoubleHistogramBuckets buckets1 = new DoubleHistogramBuckets("emp_no", 0, 150000, 4);
        JsonGroups<JsonGroups<Count>> heatmap = db.histogram2D(
                new ColumnDescription("salary", ContentsKind.Integer),
                new ColumnDescription("emp_no", ContentsKind.Integer),
                buckets0, buckets1, null, null, null);
        Assert.assertNotNull(heatmap);
        int xBucketCount = heatmap.perBucket.size();
        int yBucketCount = heatmap.perBucket.get(0).perBucket.size();
        Assert.assertEquals(8, xBucketCount);
        Assert.assertEquals(4, yBucketCount);
        long total = 0;
        for (int i = 0; i < xBucketCount; i++)
            for (int j = 0; j < yBucketCount; j++)
                total += heatmap.perBucket.get(i).perBucket.get(j).count;
        Assert.assertEquals(950571, total);
        db.disconnect();
    }

    @Test
    public void testMysqlQuantizedHeatmapSelection() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DoubleColumnQuantization q0 = new DoubleColumnQuantization("salary",100, 0, 200000);
        DoubleColumnQuantization q1 = new DoubleColumnQuantization("emp_no", 100, 0, 500000);
        DoubleHistogramBuckets buckets0 = new DoubleHistogramBuckets("salary", 0, 200000, 8);
        DoubleHistogramBuckets buckets1 = new DoubleHistogramBuckets("emp_no", 0, 150000, 4);
        JsonGroups<JsonGroups<Count>> heatmap = db.histogram2D(
                new ColumnDescription("salary", ContentsKind.Integer),
                new ColumnDescription("emp_no", ContentsKind.Integer),
                buckets0, buckets1, null, q0, q1);
        Assert.assertNotNull(heatmap);
        int xBucketCount = heatmap.perBucket.size();
        int yBucketCount = heatmap.perBucket.get(0).perBucket.size();

        Assert.assertEquals(8, xBucketCount);
        Assert.assertEquals(4, yBucketCount);
        long total = 0;
        for (int i = 0; i < xBucketCount; i++)
            for (int j = 0; j < yBucketCount; j++)
                total += heatmap.perBucket.get(i).perBucket.get(j).count;
        Assert.assertEquals(950571, total);
        db.disconnect();
    }

    @Test
    public void testMysqlNumericQuantizedHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        DoubleHistogramBuckets buckets = new DoubleHistogramBuckets("salary", 0, 200000, 8);
        DoubleColumnQuantization quantization = new DoubleColumnQuantization("salary", 5, 0, 200000);
        IGroup<Count> histogram = db.histogram(
                new ColumnDescription("salary", ContentsKind.Integer), buckets,
                null, quantization, 2844047);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(8, histogram.size());
        Assert.assertEquals(0, histogram.getMissing().count);
        Assert.assertEquals(0, histogram.getBucket(0).count);
        Assert.assertEquals(0, histogram.getBucket(7).count);
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(2844047, total);
        db.disconnect();
    }

    @Test
    public void testMysqlStringHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        String[] boundaries = new String[] { "A", "F", "K", "P", "T", "X" };
        StringHistogramBuckets buckets = new StringHistogramBuckets("first_name", boundaries);
        IGroup<Count> histogram = db.histogram(
                new ColumnDescription("first_name", ContentsKind.String), buckets, null, null, 300024);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(6, histogram.size());
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(300024, total);
        db.disconnect();
    }


    @Test
    public void testMysqlQuantizedStringHistogram() throws SQLException {
        JdbcConnectionInformation conn = this.mySqlTestDbConnection();
        conn.table = "employees";
        JdbcDatabase db = new JdbcDatabase(conn);
        try {
            db.connect();
        } catch (Exception e) {
            // This will fail if a database is not deployed, but we don't want to fail the test.
            this.ignoringException("Cannot connect to database", e);
            return;
        }
        String[] boundaries = new String[] { "A", "F", "K", "P", "T", "X" };
        StringHistogramBuckets buckets = new StringHistogramBuckets("first_name", boundaries);
        String[] letters = new String[26];
        for (char c = 'A'; c <= 'Z'; c++)
            letters[c - 'A'] = Character.toString(c);
        StringColumnQuantization quantization = new StringColumnQuantization("first_name", letters, "z");
        IGroup<Count> histogram = db.histogram(
                new ColumnDescription("first_name", ContentsKind.String), buckets, null, quantization, 300024);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(6, histogram.size());
        long total = 0;
        for (int i = 0; i < histogram.size(); i++)
            total += histogram.getBucket(i).count;
        Assert.assertEquals(300024, total);
        db.disconnect();
    }
}

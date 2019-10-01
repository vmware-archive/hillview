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

import org.hillview.sketches.DoubleHistogramBuckets;
import org.hillview.sketches.StringHistogramBuckets;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.utils.Utilities;

import java.util.HashMap;

/**
 * Base abstract class that handles various specifics of JDBC driver requirements.
 */
abstract class JdbcConnection {
    /**
     * Separates options from each other in rul.
     */
    private final char urlOptionsSeparator;
    /**
     * Separates options from the rest of the url.
     */
    private final char urlOptionsBegin;
    public final JdbcConnectionInformation info;
    private final HashMap<String, String> params = new HashMap<String, String>();

    static JdbcConnection create(JdbcConnectionInformation conn) {
        if (Utilities.isNullOrEmpty(conn.databaseKind))
            throw new RuntimeException("Database kind cannot be empty");
        switch (conn.databaseKind) {
            case "mysql":
                return new MySqlJdbcConnection(conn);
            case "impala":
                return new ImpalaJdbcConnection(conn);
            default:
                throw new RuntimeException("Unsupported JDBC database kind " + conn.databaseKind);
        }
    }

    /**
     * Construct the URL used to connect to the database.
     */
    public abstract String getURL();

    /**
     * Construct the query string to read the specified table.
     * @param table     Table to read.
     * @param rowCount  Number of rows to read.
     * @return          A SQL query string that reads the specified number of rows.
     */
    public abstract String getQueryToReadTable(String table, int rowCount);

    String getQueryToReadSize(String table) {
        return "SELECT COUNT(*) FROM " + table;
    }

    String getQueryForDistinctCount(String table, String column) {
        return "SELECT COUNT(DISTINCT " + column + ") FROM " + table;
    }

    void addBaseUrl(StringBuilder urlBuilder) {
        urlBuilder.append("jdbc:");
        urlBuilder.append(info.databaseKind);
        urlBuilder.append("://");
        urlBuilder.append(info.host);
        if (info.port >= 0) {
            urlBuilder.append(":");
            urlBuilder.append(info.port);
        }
        urlBuilder.append("/");
        urlBuilder.append(info.database);
    }

    /**
     * Append all query parameters to a StringBuilder which is used
     * to construct a query url.
     * @param urlBuilder  StringBuilder used to construct the query url.
     */
    void appendParametersToUrl(StringBuilder urlBuilder) {
        urlBuilder.append(this.urlOptionsBegin);
        boolean first = true;
        for (String p: this.params.keySet()) {
            if (!first)
                urlBuilder.append(this.urlOptionsSeparator);
            first = false;
            urlBuilder.append(p);
            urlBuilder.append("=");
            urlBuilder.append(this.params.get(p));
        }
    }

    void addParameter(String param, String value) {
        this.params.put(param, value);
    }

    JdbcConnection(char urlOptionsSeparator, char urlOptionsBegin,
                   JdbcConnectionInformation info) {
        this.urlOptionsSeparator = urlOptionsSeparator;
        this.urlOptionsBegin = urlOptionsBegin;
        this.info = info;
    }

    String getQueryToComputeFreqValues(String table, Schema schema, int minCt) {
        StringBuilder builder = new StringBuilder();
        String ctcol = schema.newColumnName("countcol");
        /*
        e.g., select gender, first_name, count(*) as ct
              from employees
              group by gender, first_name
              order by count desc
              having ct > minCt
         */
        boolean first = true;
        StringBuilder cols = new StringBuilder();
        for (String col : schema.getColumnNames()) {
            if (!first)
                cols.append(", ");
            first = false;
            cols.append(col);
        }
        builder.append("select ").append(cols.toString()).append(", count(*) AS ").append(ctcol)
                .append(" from ").append(table)
                .append(" group by ").append(cols.toString())
                .append(" having ").append(ctcol).append(" > " ).append(minCt)
                .append(" order by ").append(ctcol).append(" desc")
                ;
        return builder.toString();
    }

    public abstract String getQueryForNumericHistogram(
            String table, ColumnDescription cd, DoubleHistogramBuckets buckets);

    public abstract String getQueryForStringHistogram(
            String table, ColumnDescription cd, StringHistogramBuckets buckets);

    /**
     * Returns a query that computes 4 values for a given numeric column.
     * @param table  Table used.
     * @param column Column name.
     * @return       A query that computes the min, max, total rows, and non-nulls in the specified column.
     *               These are returned in columns min, max, total and nonnulls respectively.
     */
    public abstract String getQueryForNumericRange(String table, String column);

    @SuppressWarnings("WeakerAccess")
    public String getQueryForCounts(String table, String column) {
        return "select COUNT(*) as total, COUNT(" + column + ") as nonnulls from " + table;
    }

    public String getQueryForDistinct(String table, String column) {
        return "SELECT DISTINCT " + column + " FROM " + table + " ORDER BY " + column;
    }
}

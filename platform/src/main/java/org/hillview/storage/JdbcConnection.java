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

import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
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
     * Construct the query string to read the connection table.
     * @param rowCount  Number of rows to read.
     * @return          A SQL query string that reads the specified number of rows.
     */
    public abstract String getQueryToReadTable(int rowCount);

    String getQueryToReadSize(@Nullable ColumnLimits columnLimits) {
        throw new UnsupportedOperationException();
    }

    String getQueryForDistinctCount(String column, @Nullable ColumnLimits columnLimits) {
        throw new UnsupportedOperationException();
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

    String getQueryToComputeFreqValues(Schema schema, int minCt, @Nullable ColumnLimits columnLimits) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a query that computes 4 values for a given numeric column.
     * @param cd Column description.
     * @param quantization  Optional quantization information for this column.
     * @param columnLimits  Limits for each column.
     * @return       A query that computes the min, max, total rows, and non-nulls in the specified column.
     *               These are returned in columns min, max, total and nonnulls respectively.
     */
    public String getQueryForNumericRange(ColumnDescription cd,
                                          @Nullable DoubleColumnQuantization quantization,
                                          @Nullable ColumnLimits columnLimits) {
        throw new UnsupportedOperationException();
    }

    public String getQueryForCounts(ColumnDescription cd, @Nullable ColumnQuantization quantization,
                                    @Nullable ColumnLimits columnLimits) {
        throw new UnsupportedOperationException();
    }

    public String getQueryForDistinct(String column) {
        Converters.checkNull(this.info.table);
        return "SELECT DISTINCT " + column + " FROM " + this.info.table + " ORDER BY " + column;
    }

    public String getQueryForHistogram(ColumnDescription cd,
                                @Nullable ColumnLimits columnLimits,
                                IHistogramBuckets buckets,
                                @Nullable ColumnQuantization quantization) {
        throw new UnsupportedOperationException();
    }

    public String getQueryForHeatmap(ColumnDescription cd0, ColumnDescription cd1,
                                     @Nullable ColumnLimits columnLimits,
                                     IHistogramBuckets buckets0, IHistogramBuckets buckets1,
                                     @Nullable ColumnQuantization quantization0,
                                     @Nullable ColumnQuantization quantization1) {
        throw new UnsupportedOperationException();
    }
}

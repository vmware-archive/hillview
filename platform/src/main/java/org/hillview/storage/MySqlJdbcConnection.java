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

import org.hillview.sketches.results.DoubleHistogramBuckets;
import org.hillview.sketches.results.StringHistogramBuckets;
import org.hillview.table.ColumnDescription;
import org.hillview.utils.Converters;

import java.time.Instant;

public class MySqlJdbcConnection extends JdbcConnection {
    MySqlJdbcConnection(JdbcConnectionInformation conn) {
        super('&', '?', conn);
    }

    @Override
    public String getQueryToReadTable(String table, int rowCount) {
        String result = "SELECT * FROM " + table;
        if (rowCount >= 0)
            result += " LIMIT " + rowCount;
        return result;
    }

    @Override
    public String getURL() {
        this.addParameter("useLegacyDatetimeCode", "true");
        this.addParameter("useSSL", "false");
        StringBuilder builder = new StringBuilder();
        this.addBaseUrl(builder);
        this.appendParametersToUrl(builder);
        return builder.toString();
    }

    @Override
    public String getQueryForNumericHistogram(
            String table, ColumnDescription cd, DoubleHistogramBuckets buckets) {
        double scale = (double)buckets.bucketCount / buckets.range;
        return "select bucket, count(bucket) from (" +
                "select CAST(FLOOR((" + cd.name + " - " + buckets.minValue + ") * " + scale + ") as UNSIGNED) as bucket" +
                " from " + table +
                " where " + cd.name + " between " + buckets.minValue + " and " + buckets.maxValue +
                ") tmp group by bucket";
    }

    private static String searchInterval(int left, int right, String[] boundaries, String column) {
        if (left == right - 1)
            return Integer.toString(left);
        int mid = left + (right - left) / 2;
        String result = "if(" + column + " < BINARY '" + boundaries[mid] + "', ";
        String recLeft = searchInterval(left, mid, boundaries, column);
        String recRight = searchInterval(mid, right, boundaries, column);
        return result + recLeft + ", " + recRight + ")";
    }

    @Override
    public String getQueryForStringHistogram(
            String table, ColumnDescription cd, StringHistogramBuckets buckets) {
        // We synthesize a binary search three
        return "select bucket, count(bucket) from (" +
                "select (" +
                searchInterval(0, buckets.getBucketCount(), buckets.leftBoundaries, cd.name) +
                ") as bucket from " + table + ") tmp" +
                " group by bucket";
    }

    @Override
    public String getQueryForNumericRange(String table, String colName) {
        return "select MIN(" + colName + ") as min, MAX(" + colName +
                ") as max, COUNT(*) as total, COUNT(" + colName + ") as nonnulls from " + table;
    }

    @Override
    public String getQueryForDistinct(String table, String column) {
        // BINARY is needed to force mysql to do a case-sensitive comparison
        return "SELECT CAST(" + column + " AS CHAR) FROM " +
                "(SELECT DISTINCT BINARY " + column + " AS " + column + " FROM " + table + " ORDER BY BINARY " + column + ") tmp";
    }

    public String getQueryForDateHistogram(String table, ColumnDescription cd, DoubleHistogramBuckets buckets) {
        Instant minDate = Converters.toDate(buckets.minValue);
        Instant maxDate = Converters.toDate(buckets.maxValue);
        String minString = minDate.toString();
        String maxString = maxDate.toString();
        double min = minDate.toEpochMilli() * 1000.0;
        double max = maxDate.toEpochMilli() * 1000.0;
        double scale = (double)buckets.bucketCount / (max - min);
        return "select bucket, count(bucket) from (" +
                "select CAST(FLOOR(TIMESTAMPDIFF(MICROSECOND, '" + minDate + "', " + cd.name + ") * " + scale + ") as UNSIGNED) as bucket from " + table +
                " where " + cd.name + " between '" + minDate + "' and '" + maxDate + "'" +
                ") tmp" +
                " group by bucket";
    }
}

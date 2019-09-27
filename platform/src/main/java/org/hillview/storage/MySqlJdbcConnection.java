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
        double scale = (double)buckets.numOfBuckets / buckets.range;
        return "select bucket, count(bucket) from (" +
                "select CAST(FLOOR((" + cd.name + " - " + buckets.minValue + ") * " + scale + ") as UNSIGNED) as bucket" +
               " from " + table + ") tmp" +
               " group by bucket";
    }

    @Override
    public String getQueryForStringHistogram(
            String table, ColumnDescription cd, StringHistogramBuckets buckets) {
        StringBuilder builder = new StringBuilder();
        builder.append("select bucket, count(bucket) from (");
        builder.append("select (CASE ");
        for (int i = 0; i < buckets.leftBoundaries.length; i++) {
            builder.append("WHEN ").append(cd.name).append(" >= '").append(buckets.leftBoundaries[i]).append("'");
            if (i < buckets.leftBoundaries.length - 1)
                builder.append(" and ").append(cd.name).append(" < '").append(buckets.leftBoundaries[i+1]).append("'");
            builder.append(" then ").append(i).append(" ");
        }
        builder.append("end) as bucket from ").append(table).append(") tmp");
        builder.append(" group by bucket");
        return builder.toString();
    }

    @Override
    public String getQueryForNumericRange(String table, String colName) {
        return "select MIN(" + colName + ") as min, MAX(" + colName +
                ") as max, COUNT(*) as total, COUNT(" + colName + ") as nonnulls from " + table;
    }
}

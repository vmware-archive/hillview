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
import org.hillview.utils.Utilities;

public class ImpalaJdbcConnection extends JdbcConnection {
    ImpalaJdbcConnection(JdbcConnectionInformation conn) {
        super(';', ';', conn);
    }

    @Override
    public String getQueryToReadTable(String table, int rowCount) {
        String result = "SELECT * FROM " + table;
        if (rowCount >= 0)
            result += " LIMIT " + rowCount;
        return result;
    }

    @Override
    public String getQueryForNumericHistogram(
            String table, ColumnDescription cd, DoubleHistogramBuckets buckets) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public String getQueryForStringHistogram(
            String table, ColumnDescription cd, StringHistogramBuckets buckets) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public String getQueryForNumericRange(String table, String column) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public String getURL() {
        this.addParameter("UseNativeQuery", "1");

        int authMechanism = 0;
        boolean hasUser = false;
        boolean hasPassword = false;

        if (!Utilities.isNullOrEmpty(info.user))
            hasUser = true;
        if (!Utilities.isNullOrEmpty(info.password))
            hasPassword = true;

        if (hasUser && hasPassword)
            authMechanism = 3;
        else if (hasUser)
            authMechanism = 2;

        this.addParameter("AuthMech", Integer.toString(authMechanism));
        this.addParameter("SSL", "1");
        //this.addParameter("LogLevel", "3");

        StringBuilder builder = new StringBuilder();
        this.addBaseUrl(builder);
        this.appendParametersToUrl(builder);
        return builder.toString();
    }
}

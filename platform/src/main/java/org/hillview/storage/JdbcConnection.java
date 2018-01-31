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

import org.hillview.utils.Utilities;

import java.util.HashMap;

/**
 * Base abstract class that handles various specifics of JDBC driver requirements.
 */
public abstract class JdbcConnection {
    public final char urlSeparator;
    public final JdbcConnectionInformation info;
    final HashMap<String, String> params = new HashMap<String, String>();

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
    public abstract String getQuery(String table, int rowCount);

    protected void addBaseUrl(StringBuilder urlBuilder) {
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
    protected void appendParametersToUrl(StringBuilder urlBuilder) {
        for (String p: this.params.keySet()) {
            urlBuilder.append(this.urlSeparator);
            urlBuilder.append(p);
            urlBuilder.append("=");
            urlBuilder.append(this.params.get(p));
        }
    }

    void addParameter(String param, String value) {
        this.params.put(param, value);
    }

    public JdbcConnection(char urlSeparator, JdbcConnectionInformation info) {
        this.urlSeparator = urlSeparator;
        this.info = info;
    }
}

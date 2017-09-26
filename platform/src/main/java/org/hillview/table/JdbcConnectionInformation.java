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

package org.hillview.table;

import org.apache.http.client.utils.URIBuilder;

import java.io.Serializable;

/**
 * This information is required to open a database connection.
 */
public class JdbcConnectionInformation implements Serializable {
    public final String host;
    public final String database;
    public final int port;
    public final String user;
    public final String password;
    public final String databaseKind;

    public JdbcConnectionInformation(String host, String database, String user, String password) {
        this.host = host;
        this.user = user;
        this.database = database;
        this.password = password;
        this.port = 3306;
        this.databaseKind = "mysql";
    }

    public String getURL() {
        URIBuilder builder = new URIBuilder();
        builder.setHost(this.host);
        builder.setPort(this.port);
        builder.setScheme("jdbc:" + this.databaseKind);
        builder.setPath(this.database);
        builder.addParameter("useSSL", "false");
        builder.addParameter("serverTimeZone", "PDT");
        return builder.toString();
    }
}

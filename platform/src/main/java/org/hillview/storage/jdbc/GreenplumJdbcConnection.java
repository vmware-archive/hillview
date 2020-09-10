/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.storage.jdbc;

import org.hillview.storage.ColumnLimits;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;

public class GreenplumJdbcConnection extends JdbcConnection {
    public static final String DRIVER = "com.pivotal.jdbc.GreenplumDriver";

    GreenplumJdbcConnection(JdbcConnectionInformation conn) {
        super(';', ';', conn);
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    String getQueryToReadSize(@Nullable ColumnLimits limits) {
        return "SELECT COUNT(*) FROM " + this.info.table;
    }

    @Override
    public String getURL() {
        this.addParameterIfNotNullOrEmpty("DatabaseName", info.database);
        if (!Utilities.isNullOrEmpty(info.user) && !Utilities.isNullOrEmpty(info.password))
            this.addParameter("AuthenticationMethod", "userIdPassword");
        this.addParameterIfNotNullOrEmpty("User", info.user);
        this.addParameterIfNotNullOrEmpty("Password", info.password);
        StringBuilder builder = new StringBuilder();
        this.addBaseUrl(builder);
        this.appendParametersToUrl(builder);
        return builder.toString();
    }

    void addBaseUrl(StringBuilder urlBuilder) {
        urlBuilder.append("jdbc:pivotal:");
        urlBuilder.append(info.databaseKind);
        urlBuilder.append("://");
        urlBuilder.append(info.host);
        if (info.port >= 0) {
            urlBuilder.append(":");
            urlBuilder.append(info.port);
        }
    }
}

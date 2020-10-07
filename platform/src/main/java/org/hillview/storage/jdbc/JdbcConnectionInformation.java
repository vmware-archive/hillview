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

import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This information is required to open a database connection.
 */
public class JdbcConnectionInformation implements Serializable {
    static final long serialVersionUID = 1;

    @Nullable
    public String host;
    @Nullable
    public String database;
    @Nullable
    public String table;
    public int port;
    @Nullable
    public String user;
    @Nullable
    public String password;
    @Nullable
    public String databaseKind;
    /**
     * If true data is loaded lazily - on demand.
     */
    public boolean lazyLoading;

    public void validate() {
        // To avoid code injection
        Utilities.checkIdentifier(this.database);
        Utilities.checkIdentifier(this.table);
    }

    @Override
    public String toString() {
        return "Connection Information:\n" +
                "  databaseKind : " + this.databaseKind + "\n" +
                "  host : " + this.host + "\n" +
                "  port : " + this.port + "\n" +
                "  database : " + this.database + "\n" +
                "  table : " + this.table + "\n" +
                "  user : " + this.user + "\n";
    }
}

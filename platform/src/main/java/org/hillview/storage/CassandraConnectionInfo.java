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

package org.hillview.storage;

public class CassandraConnectionInfo extends JdbcConnectionInformation {
    static final long serialVersionUID = 1;

    // Port for establising probe connection
    public int jmxPort;
    public String cassandraRootDir;

    public CassandraConnectionInfo (String cassandraRootDir, String host, int jmxPort,
        int nativePort, String keyspace, String table, String user, String password){
        this.cassandraRootDir = cassandraRootDir;
        this.host = host;
        this.jmxPort = jmxPort;
        this.port = nativePort;
        this.database = keyspace;
        this.table = table;
        this.databaseKind = "Cassandra";
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString() {
        return this.cassandraRootDir + "/" + this.port;
    }
}

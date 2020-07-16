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

/**
 * This information is required to open a Cassandra database connection.
 */
public class CassandraConnectionInfo extends JdbcConnectionInformation {
    static final long serialVersionUID = 1;

    /** Port for establishing probe connection */
    public int jmxPort;
    /** Local Cassandra installation directory (can be found at bin/install-cassandra.sh) */
    public String cassandraRootDir;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CassandraConnectionInfo:\n");
        sb.append("  JdbcConnectionInformation: " + super.toString());
        sb.append("  databaseKind : " + this.databaseKind + "\n");
        sb.append("  host : " + this.host + "\n");
        sb.append("  cassandraRootDir : " + this.cassandraRootDir + "\n");
        sb.append("  port : " + this.port + "\n");
        sb.append("  jmxPort : " + this.jmxPort + "\n");
        sb.append("  database : " + this.database + "\n");
        sb.append("  table : " + this.table + "\n");
        sb.append("  user : " + this.user + "\n");
        return sb.toString();
    }
}

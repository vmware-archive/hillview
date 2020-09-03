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
 * This information is required to open a Hive database connection; and to read
 * the underlying HDFS file.
 */
public class HiveConnectionInfo extends JdbcConnectionInformation {
    static final long serialVersionUID = 1;
    
    /** The hostname/IP addresses of hdfs nodes in the cluster, separated by comma */
    public String hdfsNodes;
    /** For establishing connection to Hadoop namenode */
    public String namenodeAddress;
    public String namenodePort;
    /** Delimiter used to parse the hdfs data */
    public String dataDelimiter;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("  hdfsNodes : " + this.hdfsNodes + "\n");
        sb.append("  namenodeAddress : " + this.namenodeAddress + "\n");
        sb.append("  namenodePort : " + this.namenodePort + "\n");
        return sb.toString();
    }
}

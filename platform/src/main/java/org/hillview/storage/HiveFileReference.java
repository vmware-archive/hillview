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

import java.io.File;
import java.net.InetAddress;
import java.sql.ResultSetMetaData;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.hillview.storage.HiveDatabase.HivePartition;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

public class HiveFileReference implements IFileReference {

    private final HiveConnectionInfo conn;
    private final UserGroupInformation hadoopUGI;
    private final Schema tableSchema;
    private final ResultSetMetaData metadataColumn;
    private final List<HivePartition> arrPartitions;
    private final List<String> hdfsInetAddresses;
    private final HiveHDFSLoader hiveLoader;
    
    public HiveFileReference(final HiveConnectionInfo conn, final UserGroupInformation hadoopUGI, Schema tableSchema,
            ResultSetMetaData metadataColumn, List<HivePartition> arrPartitions, List<String> hdfsInetAddresses) {
        this.conn = conn;
        this.hadoopUGI = hadoopUGI;
        this.tableSchema = tableSchema;
        this.metadataColumn = metadataColumn;
        this.arrPartitions = arrPartitions;
        this.hdfsInetAddresses = hdfsInetAddresses;
        hiveLoader = new HiveHDFSLoader(this.conn, this.hadoopUGI, this.tableSchema, this.metadataColumn,
                this.arrPartitions, this.hdfsInetAddresses);
    }

    @Override
    public ITable load() {
        TextFileLoader loader;
        loader = hiveLoader;
        return Converters.checkNull(loader.load());
    }

    @Override
    public long getSizeInBytes() {
        return hiveLoader.getFileSizeInBytes();
    }
}
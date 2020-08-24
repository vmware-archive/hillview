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

package org.hillview.maps;

import org.apache.hadoop.security.UserGroupInformation;
import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;
import org.hillview.storage.HiveConnectionInfo;
import org.hillview.storage.HiveDatabase;
import org.hillview.storage.HiveFileReference;
import org.hillview.storage.IFileReference;
import org.hillview.storage.HiveDatabase.HivePartition;
import org.hillview.table.Schema;

import javax.annotation.Nullable;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FindHiveHDFSFilesMap implements IMap<Empty, List<IFileReference>> {
    static final long serialVersionUID = 1;
    private final HiveConnectionInfo conn;

    public FindHiveHDFSFilesMap(HiveConnectionInfo conn) {
        this.conn = conn;
    }

    @Override
    public List<IFileReference> apply(@Nullable Empty empty) {
        // TODO: Split the load, the root node should collect the info and the workers just load the data
        List<IFileReference> result = new ArrayList<IFileReference>();
        HiveDatabase db = new HiveDatabase(this.conn);
        UserGroupInformation hadoopUGI = db.getHadoopUGI();
        Schema tableSchema = db.getTableSchema();
        ResultSetMetaData metadataColumn = db.getMetadataColumn();
        List<HivePartition> arrPartitions = db.getArrPartitions();
        List<String> hdfsInetAddresses = db.getHdfsInetAddresses();

        try {
            db.closeConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        result.add(new HiveFileReference(this.conn, hadoopUGI, tableSchema, metadataColumn, arrPartitions,
                hdfsInetAddresses));
        return result;
    }
}
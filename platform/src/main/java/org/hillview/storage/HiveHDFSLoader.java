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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.hillview.storage.HiveDatabase.FileLocality;
import org.hillview.storage.HiveDatabase.HivePartition;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.Schema;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

public class HiveHDFSLoader extends TextFileLoader {

    private final Configuration hdfsConf;
    private final HiveConnectionInfo info;
    private final UserGroupInformation hadoopUGI;

    private final List<HivePartition> arrPartitions;
    private final List<InetAddress> hdfsInetAddresses;
    private final boolean lazyLoading;

    private final ResultSetMetaData metadataColumn;
    private final InetAddress localHDFSNode;
    private final Schema actualSchema;

    public HiveHDFSLoader(HiveConnectionInfo info, UserGroupInformation hadoopUGI, Schema tableSchema,
            ResultSetMetaData metadataColumn, List<HivePartition> arrPartitions,
            List<InetAddress> hdfsInetAddresses) {
        super(info.table);
        this.info = info;
        this.hadoopUGI = hadoopUGI;
        this.actualSchema = tableSchema;
        this.arrPartitions = arrPartitions;
        this.hdfsInetAddresses = hdfsInetAddresses;
        this.metadataColumn = metadataColumn;
        this.lazyLoading = this.info.lazyLoading;
        
        try {
            this.localHDFSNode = HiveDatabase.discoverLocalHDFSInterface(this.hdfsInetAddresses);
            this.hdfsConf = HiveDatabase.initHDFSConfig(this.localHDFSNode, this.info.hadoopUsername,
                    this.info.namenodePort);
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing HiveHDFSLoader partitions", "{0}", this.toString());
            throw new RuntimeException(e);
        }
    }

    private List<IAppendableColumn> createColumns() {
        List<ColumnDescription> cols = this.actualSchema.getColumnDescriptions();
        List<IAppendableColumn> result = new ArrayList<IAppendableColumn>(this.actualSchema.getColumnCount());
        for (ColumnDescription cd : cols) {
            result.add(BaseListColumn.create(cd));
        }
        return result;
    }

    public ITable load() {
        int columnSize = this.actualSchema.getColumnCount();
        List<IAppendableColumn> columns = this.createColumns();
        
        try {
            ITable table = this.hadoopUGI.doAs(new PrivilegedExceptionAction<ITable>() {
                @Override
                public ITable run() throws Exception {
                    FileSystem fs = FileSystem.get(hdfsConf);
                    int hasPartition = 1;
                    for (HivePartition hivePartition : arrPartitions) {
                        // When the table has no partition, the field will be empty
                        if (hivePartition.field.isEmpty())
                            hasPartition = 0;
                        // A single partition could consists of multiple hdfs files
                        for (FileLocality file : hivePartition.files) {
                            // Only load the hdfs file if the local hdfs is the 1st main replica
                            if (file.locality.get(0).equals(localHDFSNode)) {
                                Path path = new Path("hdfs://localhost:9000" + file.fullPath);
                                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                                String line;
                                line = br.readLine();
                                while (line != null) {
                                    String items[] = line.split(info.dataDelimiter, -1);
                                    if (items.length + hasPartition < columnSize) {
                                        HillviewLogger.instance.error("Invalid parsing caused by invalid delimiter",
                                                "{0}", this.toString());
                                        throw new RuntimeException("Invalid parsing caused by invalid delimiter");
                                    }
                                    int colIndex = 0;
                                    IAppendableColumn col;
                                    int reducer = 0;
                                    while (colIndex < columnSize) {
                                        col = columns.get(colIndex);
                                        String data;
                                        if (hivePartition.colId == colIndex) {
                                            data = hivePartition.value;
                                            reducer = 1;
                                        } else {
                                            data = items[colIndex - reducer];
                                        }
                                        
                                        if (data.isEmpty()) {
                                            col.appendMissing();
                                        } else {
                                            int colType = metadataColumn.getColumnType(colIndex + 1);
                                            switch (colType) {
                                                case Types.BOOLEAN:
                                                case Types.BIT:
                                                    col.append(data);
                                                    break;
                                                case Types.TINYINT:
                                                case Types.SMALLINT:
                                                case Types.INTEGER:
                                                    col.append(Integer.parseInt(data));
                                                    break;
                                                case Types.BIGINT:
                                                case Types.FLOAT:
                                                case Types.REAL:
                                                case Types.DOUBLE:
                                                case Types.NUMERIC:
                                                case Types.DECIMAL:
                                                    col.append(Double.parseDouble(data));
                                                    break;
                                                case Types.CHAR:
                                                case Types.VARCHAR:
                                                case Types.LONGVARCHAR:
                                                case Types.NCHAR:
                                                case Types.NVARCHAR:
                                                case Types.LONGNVARCHAR:
                                                case Types.SQLXML:
                                                    col.append(data);
                                                    break;
                                                case Types.DATE:
                                                case Types.TIME:
                                                case Types.TIMESTAMP: {
                                                    Timestamp ts = new Timestamp(Timestamp.parse(data));
                                                    LocalDateTime ldt = ts.toLocalDateTime();
                                                    col.append(Converters.toDouble(ldt));
                                                    break;
                                                }
                                                case Types.TIME_WITH_TIMEZONE:
                                                case Types.TIMESTAMP_WITH_TIMEZONE:
                                                    Timestamp ts = new Timestamp(Timestamp.parse(data));
                                                    Instant instant = ts.toInstant();
                                                    col.append(Converters.toDouble(instant));
                                                    break;
                                                case Types.BINARY:
                                                case Types.VARBINARY:
                                                case Types.LONGVARBINARY:
                                                case Types.NULL:
                                                case Types.OTHER:
                                                case Types.JAVA_OBJECT:
                                                case Types.DISTINCT:
                                                case Types.STRUCT:
                                                case Types.ARRAY:
                                                case Types.BLOB:
                                                case Types.CLOB:
                                                case Types.REF:
                                                case Types.DATALINK:
                                                case Types.ROWID:
                                                case Types.NCLOB:
                                                case Types.REF_CURSOR:
                                                default:
                                                    throw new RuntimeException("Unhandled column type " + colType);
                                            }
                                        }
                                        colIndex++;
                                    }
                                    line = br.readLine();
                                }
                                br.close();
                            }
                        }
                    }
                    fs.close();
                    return new Table(columns, info.table, null);
                }
            });
            return table;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
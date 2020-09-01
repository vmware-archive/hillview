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
import java.io.IOException;
import java.io.InputStreamReader;
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

    private final List<HivePartition> arrPartitions;
    private final List<String> hdfsInetAddresses;

    private final ResultSetMetaData metadataColumn;
    private final String localHDFSNode;
    private final Schema actualSchema;

    public HiveHDFSLoader(HiveConnectionInfo info, Schema tableSchema,
            ResultSetMetaData metadataColumn, List<HivePartition> arrPartitions,
            List<String> hdfsInetAddresses) {
        super(info.table);
        this.info = info;
        this.actualSchema = tableSchema;
        this.arrPartitions = arrPartitions;
        this.hdfsInetAddresses = hdfsInetAddresses;
        this.metadataColumn = metadataColumn;
        
        try {
            this.localHDFSNode = HiveDatabase.discoverLocalHDFSInterface(this.hdfsInetAddresses);
            this.hdfsConf = HiveDatabase.initHDFSConfig(this.localHDFSNode, this.info.namenodePort);
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

    public ITable newLoad() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            FileSystem fs = FileSystem.get(conf);
            // Hadoop DFS Path - Input file
            Path inFile = new Path("hdfs://localhost:9000/user/hive/warehouse/invites/ds=2008-08-08/kv5.txt");
            // Check if input is valid
            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
                throw new IOException("Input file not found");
            }
            // open and read from file
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));
            String line = br.readLine();
            while (line != null) {
                System.out.println(line);
                line = br.readLine();
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ITable load() {
        int columnSize = this.actualSchema.getColumnCount();
        List<IAppendableColumn> columns = this.createColumns();

        try {
            FileSystem fs = FileSystem.get(this.hdfsConf);
            boolean hasPartition = true;
            for (HivePartition hivePartition : HiveHDFSLoader.this.arrPartitions) {
                // When the table has no partition, the field will be empty
                if (hivePartition.field.isEmpty())
                    hasPartition = false;
                for (FileLocality file : hivePartition.files) {
                    // Only load the hdfs file if the local hdfs node is the 1st (main) replica
                    if (file.locality.get(0).equals(HiveHDFSLoader.this.localHDFSNode)) {
                        Path hdfsFilePath = new Path("hdfs://" + "10.1.1.2" + ":9000" + file.fullPath);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsFilePath)));
                        String line = br.readLine();
                        while (line != null) {
                            String arrData[] = line.split(HiveHDFSLoader.this.info.dataDelimiter, -1);
                            int currColumnIdx = 0;
                            IAppendableColumn col;
                            // To account the partitioned field 
                            int partitionedFieldInserted = 0;
                            while (currColumnIdx < columnSize && currColumnIdx < arrData.length) {
                                col = columns.get(currColumnIdx);
                                String data;
                                if (hasPartition && hivePartition.colId == currColumnIdx) {
                                    // When the table has a partitioned field, that field will not be writen 
                                    // in the hdfs file, thus we need to input the value manually
                                    data = hivePartition.value;
                                    partitionedFieldInserted = 1;
                                } else {
                                    data = arrData[currColumnIdx - partitionedFieldInserted];
                                }
                                // getColumnType() start the index from 1 instead of 0, thus we add + 1
                                HiveHDFSLoader.this.appendData(col, data, 
                                        HiveHDFSLoader.this.metadataColumn.getColumnType(currColumnIdx + 1));
                                currColumnIdx++;
                            }

                            // This will handle the case where we have to add some value(s) near the end
                            while (currColumnIdx != columnSize) {
                                col = columns.get(currColumnIdx);
                                if (hasPartition && hivePartition.colId == currColumnIdx)
                                    // will add the value of partitioned column
                                    HiveHDFSLoader.this.appendData(col, hivePartition.value,
                                            HiveHDFSLoader.this.metadataColumn.getColumnType(currColumnIdx));
                                else
                                    col.appendMissing();
                                currColumnIdx++;
                            }
                            line = br.readLine();
                        }
                        br.close();
                    }
                }
            }
            fs.close();
            return new Table(columns, HiveHDFSLoader.this.info.table, null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void appendData(IAppendableColumn col, String data, int colType) {
        if (data.isEmpty()) {
            col.appendMissing();
        } else {
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
    }

    public Long getFileSizeInBytes() {
        Long totalSize = 0L;
        for (HivePartition hivePartition : HiveHDFSLoader.this.arrPartitions) {
            for (FileLocality file : hivePartition.files) {
                // Only get the size if the local hdfs node is the 1st (main) replica
                if (file.locality.get(0).equals(HiveHDFSLoader.this.localHDFSNode))
                    totalSize += file.size;
            }
        }
        return totalSize;
    }

    public String toString() {
        StringBuilder result = new StringBuilder("Hive HDFS Loader : ");
        result.append(System.lineSeparator()).append("  Reading local hdfs node : ").append(this.localHDFSNode);
        result.append(System.lineSeparator()).append("  Partition's size to read  : ").append(this.arrPartitions.size());
        return result.toString();
    }
}
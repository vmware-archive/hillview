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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UserGroupInformation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.utils.HillviewLogger;

public class HiveDatabase{

    private final HiveConnectionInfo info;
    private final Connection hiveConn;
    private final Configuration hdfsConf;
    public final List<InetAddress> hdfsInetAddresses;
    private final InetAddress localHDFSNode;
    public final UserGroupInformation hadoopUGI;
    public final Schema tableSchema;
    public final List<HivePartition> arrPartitions;
    private final DFSClient hadoopDfsClient;

    public ResultSetMetaData metadataColumn;
    public String discoveredDelimiter = "default: \\u0001";
    public String hdfsPath = "";
    public boolean isPartitionedTable = false;

    public HiveDatabase(HiveConnectionInfo info) {
        this.info = info;
        try {
            this.hiveConn = DriverManager.getConnection("jdbc:hive2://" + this.info.host + ":" + this.info.port + 
                    "/" + this.info.database, this.info.user, this.info.password);
            this.discoverTableProperties();
            this.hdfsInetAddresses = this.convertToInetAddresses();
            this.localHDFSNode = HiveDatabase.discoverLocalHDFSInterface(this.hdfsInetAddresses);
            this.hdfsConf = HiveDatabase.initHDFSConfig(this.localHDFSNode, this.info.hadoopUsername, this.info.namenodePort);
            this.hadoopDfsClient = new DFSClient(
                    new InetSocketAddress(this.localHDFSNode.getHostAddress(), Integer.parseInt(this.info.namenodePort)),
                    this.hdfsConf);
            this.hadoopUGI = UserGroupInformation.createRemoteUser(this.info.hadoopUsername);
            this.tableSchema = this.discoverTableSchema();
            this.arrPartitions = this.discoverPartitions();
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing CassandraDatabase partitions", "{0}",
                    this.info.toString());
            throw new RuntimeException(e);
        }
    }

    private void discoverTableProperties() throws SQLException {
        ResultSet rs = this.executeQuery("SHOW CREATE TABLE " + this.info.table);
        while (rs.next()) {
            if (rs.getString(1).contains("LOCATION")) {
                // It looks like this: 'hdfs://localhost:9000/user/hive/warehouse/invites'
                // we just need the /user/hive/warehouse/invites
                rs.next();
                String[] items = rs.getString(1).replace("'", "").replace(" ", "")
                        .split(this.info.namenodePort);
                this.hdfsPath = items[1];
            } else if (rs.getString(1).contains("field.delim")) {
                // It looks like this: 'field.delim'='/t',
                // we just need the /t
                String[] items = rs.getString(1).replace("'", "").replace(" ", "")
                        .replace(",", "").split("=");
                // This trueDelimiter is not accurate, for example if the user specify "/t" as 
                // the delimiter, for some reason the delimiter at hdfs is just "/". Thus, we will
                // rely on user input to specify it, they can find the delimmiter by checking the hdfs file.
                this.discoveredDelimiter = "<" + items[1] + ">";
            } else if (rs.getString(1).contains("PARTITIONED BY")) {
                this.isPartitionedTable = true;
            }
        }
    }

    public static InetAddress discoverLocalHDFSInterface(List<InetAddress> hdfsInetAddresses) throws SocketException {
        Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface netint : Collections.list(nets)) {
            Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
            for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                if (hdfsInetAddresses.contains(inetAddress))
                    return inetAddress;
            }
        }
        throw new RuntimeException("Can't find local HDFS interface from hdfsNodes");
    }

    private List<InetAddress> convertToInetAddresses() throws UnknownHostException {
        String[] nodes = this.info.hdfsNodes.replaceAll(" ", "").split(",");
        List<InetAddress> inetAddresses = new ArrayList<>();
        for (String node : nodes) {
            inetAddresses.add(InetAddress.getByName(node));
        }
        return inetAddresses;
    }

    public static Configuration initHDFSConfig(InetAddress localHDFSNode, String hadoopUsername, 
            String namenodePort) throws SQLException {
        Configuration hdfsConf = new Configuration(false);
        hdfsConf.set("fs.defaultFS", "hdfs://" + localHDFSNode.getHostAddress() + ":" + namenodePort);
        hdfsConf.set("fs.default.name", hdfsConf.get("fs.defaultFS"));
        hdfsConf.set("hadoop.job.ugi", hadoopUsername);
        return hdfsConf;
    }

    public void closeConnection() throws SQLException {
        this.hiveConn.close();
    }

    public static class HiveTable {
        public final String name;
        public final String type;

        public HiveTable(String name, String type) {
            this.name = name;
            this.type = type;
        }
        
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append(this.name).append("[").append(this.type).append("]");
            return result.toString();
        }
    }

    public static class FileLocality {
        public final String fullPath;
        public final List<InetAddress> locality;

        public FileLocality(String fullPath, List<InetAddress> locality) {
            this.fullPath = fullPath;
            this.locality = locality;
        }

        public String toString() {
            StringBuilder result = new StringBuilder("\tfullPath : " + this.fullPath);
            result.append(System.lineSeparator()).append("\t").append("locality : ").append(locality.toString());
            return result.toString();
        }
    }

    public static class HivePartition {
        public final String field;
        public final int colId;
        public final String value;
        public List<FileLocality> files;

        public HivePartition(int colId, String field, String value, List<FileLocality> files) {
            this.colId = colId;
            this.field = field;
            this.value = value;
            this.files = files;
        }
        
        public String toString() {
            StringBuilder result = new StringBuilder(System.lineSeparator());
            result.append("partition : ").append(this.field).append("='").append(this.value).append("'");
            result.append("\n    colId : ").append(this.colId);
            for (FileLocality file : this.files) {
                result.append(System.lineSeparator()).append(file.toString());
            }
            return result.toString();
        }
    }

    private ResultSet executeQuery(String query) throws SQLException {
        return this.hiveConn.createStatement().executeQuery(query);
    }
    
    private Schema getSchema(ResultSetMetaData meta) {
        try {
            Schema result = new Schema();
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnDescription cd = JdbcDatabase.getDescription(meta, i);
                result.append(cd);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Schema discoverTableSchema() throws SQLException {
        ResultSet rs = this.executeQuery("SELECT * FROM " + this.info.table + " LIMIT 0" );
        this.metadataColumn = rs.getMetaData();
        return this.getSchema(metadataColumn);
    }

    private String getPartitionFullPath(String tableName, String partition) throws SQLException {
        ResultSet rs = this.executeQuery("DESCRIBE FORMATTED " + tableName + " partition (" + partition + ")");
        String result = null;
        while (rs.next()) {
            if (rs.getString(1).contains("Location")) {
                result = rs.getString(2);
                // Split the result to get the path only
                // The pure result looks like: hdfs://localhost:9000/user/hive/warehouse/invites/ds=2008-08-15
                result = result.split(this.info.namenodePort)[1];
                break;
            }
        }
        rs.close();
        return result;
    }

    private List<FileLocality> discoverAllFilesInPartition(String partitionPath)
            throws Exception {
        Configuration conf = this.hdfsConf;
        DFSClient dfsClient = this.hadoopDfsClient;
        String nameNodePort = this.info.namenodePort;

        List<FileLocality> arrFileLocality = this.hadoopUGI.doAs(new PrivilegedExceptionAction<List<FileLocality>>() {
            @Override
            public List<FileLocality> run() throws Exception {
                FileSystem fs = FileSystem.get(conf);
                FileStatus[] status = fs.listStatus(new Path(partitionPath));
                List<FileLocality> arrFileLocality = new ArrayList<>();
                LocatedBlocks blocks = null;
                List<InetAddress> arrIPAddr = new ArrayList<>();
                for (int i = 0; i < status.length; i++) {
                    String partitionItem = status[i].getPath().toString().split(nameNodePort)[1];
                    // Will get the replica that stores the partitionItem
                    blocks = dfsClient.getNamenode().getBlockLocations(partitionItem, 0, Long.MAX_VALUE);
                    DatanodeInfo[] locations = blocks.get(0).getLocations();
                    for (DatanodeInfo datanodeInfo : locations) {
                        arrIPAddr.add(InetAddress.getByName(datanodeInfo.getIpAddr()));
                    }
                    arrFileLocality.add(new FileLocality(partitionItem, arrIPAddr));
                }
                fs.close();
                return arrFileLocality;
            }
        });
        return arrFileLocality;
    }

    private int getIdOfPartitionedColumn(String partitionedColumn) throws Exception {
        for (int i = 0; i < this.metadataColumn.getColumnCount(); i++) {
            if (this.metadataColumn.getColumnName(i+1).equals(partitionedColumn))
                return i;
        }
        throw new RuntimeException("Can't find partitioned column in metadata");
    }

    private List<HivePartition> discoverPartitions() throws Exception {
        List<HivePartition> arrPartitions = new ArrayList<>();
        if (!this.isPartitionedTable) {
            // Create a single partition that will load the table content
            arrPartitions.add(new HivePartition(-1, "", "", this.discoverAllFilesInPartition(this.hdfsPath)));
        } else {
            ResultSet rs = this.executeQuery("SHOW PARTITIONS " + this.info.table);
            while (rs.next()) {
                String[] words = rs.getString(1).split("=");
                String partitionPath = getPartitionFullPath(this.info.table, words[0] + "='" + words[1] + "'");
                List<FileLocality> arrFileLocality = discoverAllFilesInPartition(partitionPath);
                int colId = getIdOfPartitionedColumn(words[0]);
                arrPartitions.add(new HivePartition(colId, words[0], words[1], arrFileLocality));
            }
            rs.close();
        }
        return arrPartitions;
    }

    public List<HivePartition> getTablePartitions() {
        return this.arrPartitions;
    }

    public String toString() {
        StringBuilder result = new StringBuilder("Table : " + this.info.table);
        result.append(System.lineSeparator()).append("\t").append(this.tableSchema);
        result.append(System.lineSeparator()).append("Provided delimiter : ").append(this.info.dataDelimiter);
        result.append(System.lineSeparator()).append("Discovered delimiter : ").append(this.discoveredDelimiter);
        result.append(System.lineSeparator()).append(this.arrPartitions.toString());
        result.append(System.lineSeparator()).append("Raw HDFS nodes : ").append(this.info.hdfsNodes);
        result.append(System.lineSeparator()).append("InetAddr of HDFS nodes: ").append(this.hdfsInetAddresses.toString());
        result.append(System.lineSeparator()).append("Local HDFS : ").append(this.localHDFSNode.toString());
        return result.toString();
    }
}
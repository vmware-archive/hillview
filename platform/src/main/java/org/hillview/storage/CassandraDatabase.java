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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Cluster.Builder;
import com.google.common.base.Throwables;

import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeProbeFactory;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

import static org.apache.commons.lang3.StringUtils.EMPTY;

public class CassandraDatabase {
    /** All SSTable always ended with this marker */
    private static final String ssTableNameMarker = "big-Data.db";
    /** The default path of Cassandra's sstableutil tool */
    private static final String ssTableUtilPathUnix = "/bin/sstableutil";
    private static final String ssTableUtilPathWindows = "\bin\sstableutil";

    public CassandraConnectionInfo info;
    // Session: Connection to cassandra cluster for executing client-side queries
    @Nullable
    private Cluster cluster;
    @Nullable
    private Session session;
    @Nullable
    private Metadata metadata;
    // Probe: Connection to cassandra node that enables Hillview to execute server-side query
    @Nullable
    private INodeProbeFactory nodeProbeFactory;
    @Nullable
    private NodeProbe probe;
    private List<CassTable> cassTables = new ArrayList<CassTable>();
    private List<TokenRange> tokenRanges = new ArrayList<TokenRange>();

    /** This class enables Hillview to execute server-side and client-side queries, such as
     * getting sstable path, list of stored tables, list of token-range, and also the row count
     * of a specific table.
     * The flow of execution to locate a set of table partitions is as follows:
     * - CassandraDatabase establishes NodeProbe connection to get full token-range and its endpoints
     * - CassandraDatabase find the sstable path by running sstableutil
     * - Finally, CassandraSSTableReader uses both output to read the local token-range
    */
    public CassandraDatabase(CassandraConnectionInfo info) {
        this.info = info;
        try {
            this.nodeProbeFactory = new NodeProbeFactory();
            this.connectLocalProbe();
            this.connectCassCluster();
            this.setStoredTableInfo();
            this.setTokenRanges();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Connection for executing client query. */
    private void connectCassCluster() {
        Builder b = Cluster.builder().addContactPoint(info.host);
        b.withPort(info.port);

        if (!Utilities.isNullOrEmpty(info.user)) {
            b.withCredentials(info.user, info.password);
        }

        this.cluster = b.build();
        this.session = this.cluster.connect();
        this.metadata = this.cluster.getMetadata();
    }

    public void closeClusterConnection() {
        if (this.session != null)
            this.session.close();
        if (this.cluster != null)
            this.cluster.close();
    }

    /** Connection for running server-side query. */
    private void connectLocalProbe() {
        try {
            if (Utilities.isNullOrEmpty(info.user)) {
                this.probe = nodeProbeFactory.create(info.host, info.jmxPort);
            } else
                this.probe = nodeProbeFactory.create(info.host, info.jmxPort,
                    info.user, info.password);
        } catch (IOException | SecurityException e) {
            Throwable rootCause = Throwables.getRootCause(e);
            HillviewLogger.instance.error("Nodetool failed to connect", info.host + ":" + info.jmxPort +
                " - " + rootCause.getClass().getSimpleName() + " : " + rootCause.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static class TokenRange {
        public String start_token;
        public String end_token;
        public List<String> endpoints = new ArrayList<>();

        public TokenRange(String rawEntry) {
            try{
                String[] arr = rawEntry.split(":");
                this.start_token = arr[1].split(",")[0];
                this.end_token = arr[2].split(",")[0];

                String rawEndpoints = arr[3].replace(", rpc_endpoints", "");
                rawEndpoints = rawEndpoints.substring(1,rawEndpoints.length()-1);
                String[] ips = rawEndpoints.split(",");
                for (String ip : ips) {
                    endpoints.add(new String(ip));
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse token range", e);
            }
        }

        public String toString() {
            return "start: " + this.start_token + ", end: " + this.end_token +
                ", endpoints: " + this.endpoints.toString();
        }
    }

    /**
     * Shows key-range (table partition) and endpoints (replica) which stores each key-range.
     * This key-range distribution is the same on every node in the cluster.
     */
    private void setTokenRanges() {
        String keyspace = this.info.database;
        try {
            TokenRange tr;
            for (String tokenRangeString : this.probe.describeRing(keyspace)) {
                tr = new TokenRange(tokenRangeString);
                this.tokenRanges.add(tr);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class CassTable {
        public String keyspace;
        public List<String> tables;

        public CassTable(String keyspace, List<String> tables) {
            this.keyspace = keyspace;
            this.tables = tables;
        }

        public String toString(){
            String result = this.keyspace;
            for (String table : this.tables) {
                result += "\n  " + table;
            }
            return result;
        }
    }

    /** The list of keyspaces and tables are obtained from cluster connection's metadata */
    private void setStoredTableInfo() {
        List<KeyspaceMetadata> keyspaces = this.metadata.getKeyspaces();
        String keyspace;
        Collection<TableMetadata> tables;
        List<String> tableNames;
        for (KeyspaceMetadata kMetadata : keyspaces) {
            keyspace = kMetadata.getName();
            tables = this.metadata.getKeyspace(keyspace).getTables();
            tableNames = tables.stream().map(tm -> tm.getName())
                .collect(Collectors.toList());
            this.cassTables.add(new CassTable(keyspace, tableNames));
        }
    }

    /** Returns list of tables stored in the cluster  */
    public List<CassTable> getStoredTableInfo() {
        return this.cassTables;
    }

    /** Given the keyspace and table name, sstableutil will return the sstable-related files.
     * Using sstableutil command is better than doing manually sstable search. Although we know
     * the sstable parent dir from cassandra.yaml, we need to rule out the outdated sstables.
     * And sstableutil is built specifically to identify which version are the most updated.
      */
    public String getSSTablePath() {
        String sstablePath = EMPTY;
        boolean isWindows = System.getProperty("os.name")
            .toLowerCase().startsWith("windows");
        ProcessBuilder builder = new ProcessBuilder();
        if (isWindows) {
            builder.command("cmd.exe", "/c", info.cassandraRootDir + ssTableUtilPathWindows + " " +
            this.info.database + " " + this.info.table + " | findstr " + ssTableNameMarker);
        } else {
            builder.command("sh", "-c", info.cassandraRootDir + ssTableUtilPathUnix + " " +
            this.info.database + " " + this.info.table + " | grep " + ssTableNameMarker );
        }
        builder.directory(new File(System.getProperty("user.home")));
        try {
            Process p = builder.start();
            // scan the output of executed commands
            Scanner s = new Scanner(p.getInputStream());
            while (s.hasNext()) {
                sstablePath = s.next();
            }
            s.close();
            if (sstablePath.isEmpty())
                throw new RuntimeException("Failed to get SSTablePath");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sstablePath;
    }

    public Long getRowCount(String keyspace, String table) {
        ResultSet result = this.session.execute("SELECT count(*) FROM " + keyspace + "." + table);
        Row r = result.one();
        return r.getLong("count");
    }

    public String toString(){
        String result = "Available Keyspaces and Tables:";
        for (CassTable cassTable : this.cassTables) {
            result += "\n" + cassTable.toString();
        }
        result += "\n\n" + "Table partition of " + this.info.database + ":";
        for (TokenRange tokenRange : this.tokenRanges) {
            result += "\n" + tokenRange.toString();
        }
        return result;
    }

}
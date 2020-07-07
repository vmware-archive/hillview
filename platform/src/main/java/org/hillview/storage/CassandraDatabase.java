/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

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

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class CassandraDatabase {

    public CassandraConnectionInfo info;
    // Session: Connection to cassandra cluster for executing client-side queries
    private Cluster cluster = null;
    private Session session = null;
    private Metadata metadata = null;
    // Probe: Connection to cassandra node that enables Hillview to execute server-side query
    private INodeProbeFactory nodeProbeFactory = null;
    private NodeProbe probe = null;
    private List<CassTable> cassTables = new ArrayList<CassTable>();
    private List<TokenRange> tokenRanges = new ArrayList<TokenRange>();

    public CassandraDatabase(CassandraConnectionInfo info) {
        this.info = info;
        try {
            this.nodeProbeFactory = new NodeProbeFactory();
            connectLocalProbe();
            connectCassCluster();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public class CassTable {
        public String keyspace;
        public List<String> tables;

        public CassTable(String keyspace, List<String> tables) {
            this.keyspace = keyspace;
            this.tables = tables;
        }
    }

    public class TokenRange {
        public String start_token;
        public String end_token;
        public List<String> endpoints = new ArrayList<>();

        public TokenRange(String rawEntry) {
            rawEntry = rawEntry.replace("TokenRange(", "");
            String[] arr = rawEntry.split(":");
            this.start_token = arr[1].split(",")[0];
            this.end_token = arr[2].split(",")[0];

            String rawEndpoints = arr[3].replace(", rpc_endpoints", "");
            rawEndpoints = rawEndpoints.substring(1,rawEndpoints.length()-1);
            String[] ips = rawEndpoints.split(",");
            for (String ip : ips) {
                endpoints.add(new String(ip));
            }
        }

        public void printTokenRange(){
            System.out.println("start: " + this.start_token + ", end: " + this.end_token +
                ", endpoints: " + endpoints.toString());
        }
    }

    /** Connection for executing client query. The default nativePort is 9042. It can be configured
     * at cassandra.yaml as native_transport_port: 9042
     */
    public void connectCassCluster() {
        Builder b = Cluster.builder().addContactPoint(info.host);
        b.withPort(parseInt(info.nativePort));

        if (!info.username.isEmpty()) {
            b.withCredentials(info.username, info.password);
        }

        this.cluster = b.build();
        this.session = this.cluster.connect();
        this.metadata = this.cluster.getMetadata();
    }

    public void closeClusterConnection() {
        session.close();
        cluster.close();
    }

    /** Connection for running server-side query. The default jmxPort is 7199. It can be configured
     * at cassandra-env.sh as JMX_PORT="7199"
     */
    public void connectLocalProbe() {
        try {
            if (info.username.isEmpty()) {
                this.probe = nodeProbeFactory.create(info.host, parseInt(info.jmxPort));
            } else
                this.probe = nodeProbeFactory.create(info.host, parseInt(info.jmxPort),
                    info.username, info.password);
        } catch (IOException | SecurityException e) {
            Throwable rootCause = Throwables.getRootCause(e);
            HillviewLogger.instance.error("nodetool: Failed to connect to '%s:%s' - %s: '%s'.",
                info.host, info.jmxPort, rootCause.getClass().getSimpleName(), rootCause.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * The actual command is describering: shows key-range (table partition) and
     * endpoints (replica) which stores each key-range. This key-range distribution is the same
     * on every node in the cluster.
     */
    public void loadTablePartition(String keyspace) {
        try {
            for (String tokenRangeString : this.probe.describeRing(keyspace)) {
                TokenRange tr = new TokenRange(tokenRangeString);
                this.tokenRanges.add(tr);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printTablePartition(){
        for (TokenRange tokenRange : this.tokenRanges) {
            tokenRange.printTokenRange();
        }
    }

    /** The list of keyspaces and tables are obtained from cluster connection's metadata */
    public void loadKeyspacesAndTables() {
        List<KeyspaceMetadata> keyspaces = this.metadata.getKeyspaces();
        String keyspace;
        for (KeyspaceMetadata kMetadata : keyspaces) {
            keyspace = kMetadata.getName();
            Collection<TableMetadata> tables = this.metadata.getKeyspace(keyspace).getTables();
            List<String> tableNames = tables.stream().map(tm -> tm.getName())
                .collect(Collectors.toList());
            cassTables.add(new CassTable(keyspace, tableNames));
        }
    }

    public List<CassTable> getCassTables() {
        return cassTables;
    }

    public void printCassTables() {
        for (CassTable cassTable : this.cassTables) {
            System.out.println("keyspace: " + cassTable.keyspace);
            for (String table : cassTable.tables) {
                System.out.println("   " + table);
            }
        }
    }

    /** Using sstableutil command is better than doing manually sstable search. Although we know
     * the sstable parent dir from cassandra.yaml, we need to rule out the outdated sstables.
     * And sstableutil is built specifically to identify which version are the most updated.
     * Given the keyspace and table name, sstableutil will return the sstable-related files.
      */
    public String getSSTablePath(String keyspace, String table) {
        ProcessBuilder builder = new ProcessBuilder("/bin/bash");
        Process p = null;
        String sstablePath = EMPTY;

        try {
            p = builder.start();
            BufferedWriter p_stdin = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
            String command = "cd " + info.cassandraRootDir + " ; ./bin/sstableutil " +
                keyspace + " " + table + " | grep big-Data.db";

            // start bash execution
            p_stdin.write(command);
            p_stdin.newLine();
            p_stdin.flush();

            // close the shell connection
            p_stdin.write("exit");
            p_stdin.newLine();
            p_stdin.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // scan the output of executed commands
        Scanner s = new Scanner(p.getInputStream());
        while (s.hasNext()) {
            sstablePath = s.next();
        }
        s.close();

        if (sstablePath.isEmpty()) {
            throw new RuntimeException("Cassandra root directory is invalid");
        }
        return sstablePath;
    }

    public Long getRowCount(String keyspace, String table) {
        ResultSet result = this.session.execute("SELECT count(*) FROM " + keyspace + "." + table);
        Row r = result.one();
        return r.getLong("count");
    }

}
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token.TokenFactory;
import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeProbeFactory;
import org.apache.cassandra.utils.FBUtilities;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import org.yaml.snakeyaml.Yaml;
import org.apache.cassandra.dht.Token;

public class CassandraDatabase {
    /** All SSTable always ended with this marker */
    public static final String ssTableFileMarker = "big-Data.db";
    /** The default path of Cassandra's sstableutil tool */
    private static final String ssTableUtilDir = "bin/sstableutil";
    private static final String cassandraYamlPath = "conf/cassandra.yaml";
    private static final String localhost = "127.0.0.1";

    public final CassandraConnectionInfo info;
    /** Session: Connection to Cassandra cluster for executing client-side queries */
    @Nullable
    private Cluster cluster;
    /**
     * Connection to Cassandra cluster for executing client-side queries
     */
    @Nullable
    private Session session;
    @Nullable
    private Metadata metadata;
    @Nullable
    private IPartitioner partitioner;
    /**
     * Probe: Connection to Cassandra node that enables Hillview to execute
     * server-side query
     */
    @Nullable
    private final INodeProbeFactory nodeProbeFactory;
    @Nullable
    private NodeProbe probe;
    private final List<CassandraTokenRange> tokenRanges;
    private final String localEndpoint;

    /**
     * This class enables Hillview to execute server-side and client-side queries,
     * such as getting sstable path, list of stored tables, list of token-range, and
     * also the row count of a specific table. The flow of execution to locate a set
     * of table partitions is as follows: - CassandraDatabase establishes NodeProbe
     * connection to get full token-range and its endpoints - CassandraDatabase find
     * the sstable path by running sstableutil - Finally, CassandraSSTableReader
     * uses both output to read the local token-range
     */
    public CassandraDatabase(CassandraConnectionInfo info) {
        this.info = info;
        try {
            this.nodeProbeFactory = new NodeProbeFactory();
            this.connectLocalProbe();
            this.connectCassCluster();
            this.tokenRanges = this.discoverTokenRanges();
            this.localEndpoint = this.discoverLocalEndpoint();
        } catch (Exception e) {
            HillviewLogger.instance.error("Failed initializing CassandraDatabase partitions", "{0}",
                    this.info.toString());
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
        this.partitioner = FBUtilities.newPartitioner(this.metadata.getPartitioner());
    }

    public void closeClusterConnection() {
        if (this.session != null)
            this.session.close();
        if (this.cluster != null)
            this.cluster.close();
    }

    /**
     * Connection for running server-side query.
     */
    private void connectLocalProbe() throws IOException {
        Converters.checkNull(this.nodeProbeFactory);
        if (Utilities.isNullOrEmpty(info.user))
            this.probe = this.nodeProbeFactory.create(CassandraDatabase.localhost, info.jmxPort);
        else
            this.probe = this.nodeProbeFactory.create(
                    CassandraDatabase.localhost, info.jmxPort, info.user, info.password);
    }

    public static class CassandraTokenRange {
        public Range<Token> tokenRange;
        public List<String> endpoints = new ArrayList<>();

        @VisibleForTesting
        public CassandraTokenRange(Token start, Token end, List<String> endpoints) {
            this.tokenRange = new Range<Token>(start, end);
            this.endpoints = endpoints;
        }

        public CassandraTokenRange(String rawEntry, IPartitioner partitioner) {
            try {
                String[] arr = rawEntry.split(":");
                String start_token = arr[1].split(",")[0];
                String end_token = arr[2].split(",")[0];
                TokenFactory tf = partitioner.getTokenFactory();

                Token start = tf.fromString(start_token);
                Token end = tf.fromString(end_token);
                this.tokenRange = new Range<Token>(start, end);

                String rawEndpoints = arr[3].replace(", rpc_endpoints", "");
                rawEndpoints = rawEndpoints.substring(1, rawEndpoints.length() - 1);
                String[] ips = rawEndpoints.split(",");
                endpoints.addAll(Arrays.asList(ips));
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse token range", e);
            }
        }

        public String toString() {
            return "tokenRange: " + this.tokenRange.toString() +
                    ", endpoints: " + this.endpoints.toString();
        }
    }

    private String discoverLocalEndpoint() throws Exception {
        Path cassandraPath = Paths.get(this.info.cassandraRootDir);
        Path cassandraYaml = Paths.get(cassandraPath.toString(), CassandraDatabase.cassandraYamlPath);
        Yaml yamlConfig = new Yaml();
        InputStream is = new FileInputStream(cassandraYaml.toFile());
        Config config = yamlConfig.loadAs(is, Config.class);
        InetAddress inet = InetAddress.getByName(config.listen_address);
        return inet.getHostAddress();
    }

    public String getLocalEndpoint() {
        return this.localEndpoint;
    }

    /**
     * Discover key-range (table partition) and endpoints (replica) which stores each
     * key-range. This key-range distribution is the same on every node in the
     * cluster.
     */
    private List<CassandraTokenRange> discoverTokenRanges() throws IOException {
        List<CassandraTokenRange> tokenRanges = new ArrayList<>();
        String keyspace = this.info.database;
        assert this.partitioner != null;
        CassandraTokenRange tr;
        for (String tokenRangeString : Converters.checkNull(this.probe).describeRing(keyspace)) {
            tr = new CassandraTokenRange(tokenRangeString, this.partitioner);
            tokenRanges.add(tr);
        }
        return tokenRanges;
    }

    public List<CassandraTokenRange> getTokenRanges() {
        return this.tokenRanges;
    }

    public static class CassTable {
        public final String keyspace;
        public final List<String> tables;

        public CassTable(String keyspace, List<String> tables) {
            this.keyspace = keyspace;
            this.tables = tables;
        }

        public String toString() {
            StringBuilder result = new StringBuilder(this.keyspace + ": [");
            for (String table : this.tables) {
                result.append(" ").append(table);
            }
            result.append("]\n");
            return result.toString();
        }
    }

    /** Returns list of tables stored in the cluster */
    public List<CassTable> getStoredTableInfo() {
        List<CassTable> cassTables = new ArrayList<>();
        List<KeyspaceMetadata> keyspaces = Converters.checkNull(this.metadata).getKeyspaces();
        for (KeyspaceMetadata kMetadata : keyspaces) {
            String keyspace = kMetadata.getName();
            Collection<TableMetadata> tables = this.metadata.getKeyspace(keyspace).getTables();
            List<String> tableNames = tables.stream().map(AbstractTableMetadata::getName).collect(Collectors.toList());
            cassTables.add(new CassTable(keyspace, tableNames));
        }
        return cassTables;
    }

    public List<String> getSSTablePath() {
        List<String> result = new ArrayList<>();
        boolean isWindows = Utilities.runningOnWindows();
        Path ssTableUtilPath = Paths.get(CassandraDatabase.ssTableUtilDir);
        Path cassandraPath = Paths.get(this.info.cassandraRootDir);

        String sstableCommand = ssTableUtilPath.toString();
        if (isWindows)
            sstableCommand += ".bat";
        
        // Given the keyspace and table name, sstableutil will return the
        // sstable-related files. Using sstableutil command is better than doing
        // manually sstable search. Although we know the sstable parent dir from
        // cassandra.yaml, we need to rule out the outdated sstables. And sstableutil is
        // built specifically to identify which version are the most updated.
        ProcessBuilder builder = new ProcessBuilder(sstableCommand, this.info.database, this.info.table);
        builder.directory(cassandraPath.toFile());
        try {
            Process p = builder.start();
            // scan the output of executed commands
            Scanner s = new Scanner(p.getInputStream());
            while (s.hasNext()) {
                String output = s.next();
                if (output.endsWith(ssTableFileMarker)) {
                    result.add(output);
                }
            }
            s.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public String toString() {
        StringBuilder result = new StringBuilder("Available Keyspaces and Tables:");
        result.append(System.lineSeparator()).append(System.lineSeparator()).append("Table partition of ")
                .append(this.info.database).append(":");
        for (CassandraTokenRange tokenRange : this.tokenRanges) {
            result.append(System.lineSeparator()).append("\t").append(tokenRange.toString());
        }
        return result.toString();
    }
}
/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import com.google.common.net.HostAndPort;
import org.hillview.*;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.management.*;
import org.hillview.maps.FindFilesMapper;
import org.hillview.maps.LoadDatabaseTableMapper;
import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.FileLoaderDescription;
import org.hillview.storage.IFileLoader;
import org.hillview.storage.JdbcConnectionInformation;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class InitialObjectTarget extends RpcTarget {
    private static final String LOCALHOST = "127.0.0.1";
    private static final String ENV_VARIABLE = "WEB_CLUSTER_DESCRIPTOR";

    @Nullable
    private IDataSet<Empty> emptyDataset = null;

    public InitialObjectTarget() {
        Empty empty = new Empty();
        // Get the base naming context
        final String value = System.getenv(ENV_VARIABLE);
        final ClusterDescription desc;
        if (value == null) {
            desc = new ClusterDescription(Collections.singletonList(HostAndPort.fromParts(LOCALHOST,
                                                                    HillviewServer.DEFAULT_PORT)));
            this.initialize(desc);
        } else {
            try {
                HillviewLogger.instance.info("Initializing cluster descriptor from file");
                final List<String> lines = Files.readAllLines(Paths.get(value), Charset.defaultCharset());
                final List<HostAndPort> hostAndPorts = lines.stream()
                                                            .map(HostAndPort::fromString)
                                                            .collect(Collectors.toList());
                desc = new ClusterDescription(hostAndPorts);
                HillviewLogger.instance.info("Backend servers", "{0}", lines);
                this.initialize(desc);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        RpcObjectManager.instance.addObject(this);
    }

    private void initialize(final ClusterDescription description) {
        final int numServers = description.getServerList().size();
        if (numServers <= 0) {
            throw new IllegalArgumentException("ClusterDescription must contain one or more servers");
        }
        HillviewLogger.instance.info("Creating parallel dataset");
        final ArrayList<IDataSet<Empty>> emptyDatasets = new ArrayList<IDataSet<Empty>>(numServers);
        description.getServerList().forEach(server -> emptyDatasets.add(new RemoteDataSet<Empty>(server)));
        this.emptyDataset = new ParallelDataSet<Empty>(emptyDatasets);
    }

    @HillviewRpc
    public void initializeCluster(RpcRequest request, RpcRequestContext context) {
        ClusterDescription description = request.parseArgs(ClusterDescription.class);
        this.initialize(description);
    }

    @HillviewRpc
    public void loadDBTable(RpcRequest request, RpcRequestContext context) {
        JdbcConnectionInformation conn = request.parseArgs(JdbcConnectionInformation.class);
        LoadDatabaseTableMapper mapper = new LoadDatabaseTableMapper(conn);
        assert this.emptyDataset != null;
        this.runMap(this.emptyDataset, mapper, TableTarget::new, request, context);
    }

    /**
     * Describes a set of files to load.  Not all fields are always used.
     */
    public static class FileSetDescription {
        /**
         * Folder where files are looked up.
         */
        String folder = "";
        /**
         * A simple shell pattern matching file names.
         */
        @Nullable
        String fileNamePattern;
        /**
         * Name of schema file in folder.  If null or empty no schema file is assumed.
         */
        @Nullable
        String schemaFile;
        /** Used to prevent caching */
        @Nullable
        String cookie;
        /**
         * If true the files are expected to have a header row.
         */
        boolean headerRow;

        @Nullable
        String getSchemaPath() {
            if (Utilities.isNullOrEmpty(this.schemaFile))
                return null;
            return Paths.get(this.folder, this.schemaFile).toString();
        }

        @Nullable
        String getRegexPattern() {
            if (this.fileNamePattern == null)
                return null;
            return Utilities.wildcardToRegex(this.fileNamePattern);
        }
    }

    @HillviewRpc
    public void testDataset(RpcRequest request, RpcRequestContext context) {
        int which = request.parseArgs(Integer.class);
        String dataFolder = "../data/";
        String fileNamePattern;
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowMissingData = true;
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        String schemaFile;
        int limit = 0;
        int replicationFactor = 1;

        IMap<Empty, List<IFileLoader>> finder;
        if (which >= 0 && which <= 1) {
            limit = which == 0 ? 0 : 1;
            dataFolder += "ontime/";
            schemaFile = "short.schema";
            fileNamePattern = "(\\d)+_(\\d)+\\.csv";
        } else if (which == 2) {
            fileNamePattern = "vrops.csv";
            schemaFile = "vrops.schema";
        } else if (which == 3) {
            fileNamePattern = "mnist.csv";
            schemaFile = "mnist.schema";
        } else if (which == 4) {
            fileNamePattern = "segmentation.csv";
            schemaFile = "segmentation.schema";
        } else {
            throw new RuntimeException("Unexpected operation " + which);
		}

        String schemaPath = Paths.get(dataFolder, schemaFile).toString();
        FileLoaderDescription.CsvFile loader = new FileLoaderDescription.CsvFile(schemaPath, config);
        finder = new FindFilesMapper(dataFolder, limit, fileNamePattern, loader);
        HillviewLogger.instance.info("Finding csv files");
        assert this.emptyDataset != null;
        this.runFlatMap(this.emptyDataset, finder, FileDescriptionTarget::new, request, context);
    }

    @HillviewRpc
    public void findCsvFiles(RpcRequest request, RpcRequestContext context) {
        FileSetDescription desc = request.parseArgs(FileSetDescription.class);
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowMissingData = true;
        config.allowFewerColumns = true;
        config.hasHeaderRow = desc.headerRow;

        String schemaPath = desc.getSchemaPath();
        FileLoaderDescription.CsvFile loader = new FileLoaderDescription.CsvFile(schemaPath, config);
        IMap<Empty, List<IFileLoader>> finder = new FindFilesMapper(desc.folder, 0, desc.getRegexPattern(), loader);
        HillviewLogger.instance.info("Finding csv files");
        assert this.emptyDataset != null;
        this.runFlatMap(this.emptyDataset, finder, FileDescriptionTarget::new, request, context);
    }

    @HillviewRpc
    public void findJsonFiles(RpcRequest request, RpcRequestContext context) {
        FileSetDescription desc = request.parseArgs(FileSetDescription.class);
        String schemaPath = desc.getSchemaPath();
        FileLoaderDescription.JsonFile loader = new FileLoaderDescription.JsonFile(schemaPath);
        IMap<Empty, List<IFileLoader>> finder = new FindFilesMapper(desc.folder, 0, desc.getRegexPattern(), loader);
        HillviewLogger.instance.info("Finding json files");
        assert this.emptyDataset != null;
        this.runFlatMap(this.emptyDataset, finder, FileDescriptionTarget::new, request, context);
    }

    @HillviewRpc
    public void findLogs(RpcRequest request, RpcRequestContext context) {
        FileLoaderDescription.LogFile loader = new FileLoaderDescription.LogFile();
        IMap<Empty, List<IFileLoader>> finder = new FindFilesMapper(".", 0, "hillview.*\\.log", loader);
        HillviewLogger.instance.info("Finding log files");
        assert this.emptyDataset != null;
        this.runFlatMap(this.emptyDataset, finder, FileDescriptionTarget::new, request, context);
    }

    @Override
    public String toString() {
        return "Initial object=" + super.toString();
    }

    //--------------------------------------------
    // Management messages

    @HillviewRpc
    public void ping(RpcRequest request, RpcRequestContext context) {
        PingSketch<Empty> ping = new PingSketch<Empty>();
        this.runSketch(Converters.checkNull(this.emptyDataset), ping, request, context);
    }

    @HillviewRpc
    public void toggleMemoization(RpcRequest request, RpcRequestContext context) {
        ToggleMemoization tm = new ToggleMemoization();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void purgeMemoization(RpcRequest request, RpcRequestContext context) {
        PurgeMemoization tm = new PurgeMemoization();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void purgeLeafDatasets(RpcRequest request, RpcRequestContext context) {
        PurgeLeafDatasets tm = new PurgeLeafDatasets();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void memoryUse(RpcRequest request, RpcRequestContext context) {
        MemoryUse tm = new MemoryUse();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void purgeDatasets(RpcRequest request, RpcRequestContext context) {
        int deleted = RpcObjectManager.instance.removeAllObjects();
        ControlMessage.Status status = new ControlMessage.Status("Deleted " + deleted + " objects");
        JsonList<ControlMessage.Status> statusList = new JsonList<ControlMessage.Status>();
        statusList.add(status);
        PartialResult<JsonList<ControlMessage.Status>> pr = new PartialResult<JsonList<ControlMessage.Status>>(statusList);
        RpcReply reply = request.createReply(Utilities.toJsonTree(pr));
        Session session = context.getSessionIfOpen();
        if (session == null)
            return;
        RpcServer.sendReply(reply, session);
        request.syncCloseSession(session);
    }
}

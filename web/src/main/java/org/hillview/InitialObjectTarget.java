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

package org.hillview;

import com.google.common.net.HostAndPort;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.management.*;
import org.hillview.maps.FindCsvFileMapper;
import org.hillview.maps.FindFilesMapper;
import org.hillview.maps.LoadDatabaseTableMapper;
import org.hillview.utils.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.storage.JdbcConnectionInformation;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class InitialObjectTarget extends RpcTarget {
    private static final String LOCALHOST = "127.0.0.1";
    private static final String ENV_VARIABLE = "WEB_CLUSTER_DESCRIPTOR";

    @Nullable
    private IDataSet<Empty> emptyDataset = null;

    InitialObjectTarget() {
        // TODO: setup logging
        //HillviewLogger.initialize("hillview-head.log");
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
    void initializeCluster(RpcRequest request, RpcRequestContext context) {
        ClusterDescription description = request.parseArgs(ClusterDescription.class);
        this.initialize(description);
    }

    // TODO: cleanup this code
    @HillviewRpc
    void loadDBTable(RpcRequest request, RpcRequestContext context) {
        Converters.checkNull(this.emptyDataset);
        JdbcConnectionInformation conn = new JdbcConnectionInformation("localhost", "employees", "mbudiu", "password");
        LoadDatabaseTableMapper mapper = new LoadDatabaseTableMapper("salaries", conn);
        this.runMap(this.emptyDataset, mapper, TableTarget::new, request, context);
    }

    // TODO: cleanup this code.
    @HillviewRpc
    void prepareFiles(RpcRequest request, RpcRequestContext context) {
        int which = request.parseArgs(Integer.class);
        Converters.checkNull(this.emptyDataset);

        String dataFolder = "../data/";
        IMap<Empty, List<CsvFileObject>> finder;
        if (which >= 0 && which <= 1) {
            int limit = which == 0 ? 0 : 1;
            finder = new FindCsvFileMapper(dataFolder, limit, "(\\d)+_(\\d)+\\.csv", "short.schema");
        } else if (which == 2) {
            finder = new FindCsvFileMapper(dataFolder, 0, "vrops.csv", "vrops.schema");
        } else if (which == 3) {
            finder = new FindCsvFileMapper(dataFolder, 0, "mnist.csv", "mnist.schema");
        } else if (which == 4) {
            finder = new FindCsvFileMapper(dataFolder, 0, "segmentation.csv", "segmentation.schema");
        } else {
            throw new RuntimeException("Unexpected operation " + which);
        }

        HillviewLogger.instance.info("Preparing files");
        this.runFlatMap(this.emptyDataset, finder, CsvFileTarget::new, request, context);
    }

    @HillviewRpc
    void findLogs(RpcRequest request, RpcRequestContext context) {
        Converters.checkNull(this.emptyDataset);
        UUID cookie = UUID.randomUUID();
        IMap<Empty, List<String>> finder = new FindFilesMapper(".", 0, "hillview.log", cookie.toString());
        HillviewLogger.instance.info("Finding logs");
        this.runFlatMap(this.emptyDataset, finder, LogFileTarget::new, request, context);
    }

    @Override
    public String toString() {
        return "Initial object, " + super.toString();
    }

    //--------------------------------------------
    // Management messages

    @HillviewRpc
    void ping(RpcRequest request, RpcRequestContext context) {
        PingSketch<Empty> ping = new PingSketch<Empty>();
        this.runSketch(Converters.checkNull(this.emptyDataset), ping, request, context);
    }

    @HillviewRpc
    void toggleMemoization(RpcRequest request, RpcRequestContext context) {
        ToggleMemoization tm = new ToggleMemoization();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    void purgeMemoization(RpcRequest request, RpcRequestContext context) {
        PurgeMemoization tm = new PurgeMemoization();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    void purgeLeftDatasets(RpcRequest request, RpcRequestContext context) {
        PurgeLeafDatasets tm = new PurgeLeafDatasets();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    void memoryUse(RpcRequest request, RpcRequestContext context) {
        MemoryUse tm = new MemoryUse();
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    void purgeDatasets(RpcRequest request, RpcRequestContext context) {
        int deleted = RpcObjectManager.instance.removeAllObjects();
        ControlMessage.Status status = new ControlMessage.Status("Deleted " + deleted + " objects");
        JsonList<ControlMessage.Status> statusList = new JsonList<ControlMessage.Status>();
        statusList.add(status);
        PartialResult<JsonList<ControlMessage.Status>> pr = new PartialResult<JsonList<ControlMessage.Status>>(statusList);
        RpcReply reply = request.createReply(Utilities.toJsonTree(pr));
        Session session = context.getSessionIfOpen();
        if (session == null)
            return;
        reply.send(session);
        request.syncCloseSession(session);
    }
}

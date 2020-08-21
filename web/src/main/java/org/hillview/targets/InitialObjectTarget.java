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

import org.apache.commons.io.FileUtils;
import org.hillview.*;
import org.hillview.sketches.PrecomputedSketch;
import org.hillview.table.PrivacySchema;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.management.*;
import org.hillview.maps.FindCassandraFilesMap;
import org.hillview.maps.FindHiveHDFSFilesMap;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.highorder.IdMap;
import org.hillview.maps.LoadDatabaseTableMap;
import org.hillview.storage.*;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import javax.websocket.Session;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * This is the first RpcTarget that is created on the front-end.  It receives the
 * RPC requests to create other targets by loading them from storage.
 */
public class InitialObjectTarget extends RpcTarget {
    static final long serialVersionUID = 1;

    private static final String LOCALHOST = "127.0.0.1";
    private static final String ENV_VARIABLE = "WEB_CLUSTER_DESCRIPTOR";

    public static final String bookmarkDirectory = "bookmark";
    public static final String bookmarkExtension = ".json";

    @Nullable
    private IDataSet<Empty> emptyDataset = null;

    public InitialObjectTarget() {
        // Get the base naming context
        final String clusterFile = System.getenv(ENV_VARIABLE);
        final HostList desc;
        if (clusterFile == null) {
            HillviewLogger.instance.info(
                    "No cluster description file specified; creating singleton");
            desc = new HostList(Collections.singletonList(HostAndPort.fromParts(LOCALHOST,
                                                                    HillviewServer.DEFAULT_PORT)));
            this.initialize(desc);
        } else {
            try {
                HillviewLogger.instance.info(
                        "Initializing cluster descriptor from file", "{0}", clusterFile);
                desc = HostList.fromFile(clusterFile);
                HillviewLogger.instance.info("Backend servers", "{0}", desc.getServerList().size());
                this.initialize(desc);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        RpcObjectManager.instance.addObject(this);
    }

    private void initialize(final HostList description) {
        this.emptyDataset = RemoteDataSet.createCluster(description, RemoteDataSet.defaultDatasetIndex);
    }

    @HillviewRpc
    public void getUIConfig(RpcRequest request, RpcRequestContext context) {
        JsonString result;
        try {
            result = new JsonString(Utilities.textFileContents("uiconfig.json"));
            result.toJsonTree();  // force parsing of the JSON -- to catch syntax errors
        } catch (Exception e) {
            HillviewLogger.instance.warn("File uiconfig.json file could not be loaded",
                    "{0}", e.getMessage());
            result = new JsonString("{}");
        }
        Converters.checkNull(this.emptyDataset);
        PrecomputedSketch<Empty, JsonString> sk = new PrecomputedSketch<Empty, JsonString>(result);
        this.runCompleteSketch(this.emptyDataset, sk, request, context);
    }

    @HillviewRpc
    public void openingBookmark(RpcRequest request, RpcRequestContext context) {
        String bookmarkFile = request.parseArgs(String.class);
        String content;
        try {
            File file = new File(InitialObjectTarget.bookmarkDirectory, bookmarkFile);
            content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Bookmark link is broken. Failed to find bookmarked content
            throw new RuntimeException(e);
        }
        Converters.checkNull(this.emptyDataset);
        PrecomputedSketch<Empty, JsonString> sk = new PrecomputedSketch<Empty, JsonString>(new JsonString(content));
        this.runCompleteSketch(this.emptyDataset, sk, request, context);
    }

    @HillviewRpc
    public void loadSimpleDBTable(RpcRequest request, RpcRequestContext context) {
        JdbcConnectionInformation conn = request.parseArgs(JdbcConnectionInformation.class);
        IMap<Empty, Empty> map = new IdMap<Empty>();
        Converters.checkNull(this.emptyDataset);
        String dir = Paths.get(Converters.checkNull(conn.databaseKind).toLowerCase(),
                Converters.checkNull(conn.database),
                conn.table).toString();

        String privacyMetadataFile = DPWrapper.privacyMetadataFile(dir);
        if (privacyMetadataFile != null) {
            PrivacySchema privacySchema = PrivacySchema.loadFromFile(privacyMetadataFile);
            this.runMap(this.emptyDataset, map,
                    (e, c) -> {
                        try {
                            return new PrivateSimpleDBTarget(conn, c, privacySchema, privacyMetadataFile);
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    }, request, context);
        } else {
            this.runMap(this.emptyDataset, map, (e, c) -> new SimpleDBTarget(conn, c, dir), request, context);
        }
    }

    @HillviewRpc
    public void findCassandraFiles(RpcRequest request, RpcRequestContext context) {
        CassandraConnectionInfo desc = request.parseArgs(CassandraConnectionInfo.class);
        HillviewLogger.instance.info("Finding SSTable files", "{0}", desc);
        IMap<Empty, List<IFileReference>> finder = new FindCassandraFilesMap(desc);
        Converters.checkNull(this.emptyDataset);
        this.runFlatMap(this.emptyDataset, finder,
                // TODO: metadata file
                (d, c) -> new FileDescriptionTarget(d, c, null), request, context);
    }

    @HillviewRpc
    public void findHiveHDFSFiles(RpcRequest request, RpcRequestContext context) {
        HiveConnectionInfo desc = request.parseArgs(HiveConnectionInfo.class);
        HillviewLogger.instance.info("Finding Hive's hdfs files", "{0}", desc);
        IMap<Empty, List<IFileReference>> finder = new FindHiveHDFSFilesMap(desc);
        Converters.checkNull(this.emptyDataset);
        this.runFlatMap(this.emptyDataset, finder, FileDescriptionTarget::new, request, context);
    }

    @HillviewRpc
    public void loadDBTable(RpcRequest request, RpcRequestContext context) {
        JdbcConnectionInformation conn = request.parseArgs(JdbcConnectionInformation.class);
        LoadDatabaseTableMap mapper = new LoadDatabaseTableMap(conn);
        Converters.checkNull(this.emptyDataset);
        String dir = Paths.get(Converters.checkNull(conn.databaseKind).toLowerCase(),
                Converters.checkNull(conn.database),
                conn.table).toString();
        this.runMap(this.emptyDataset, mapper, (d, c) -> new TableTarget(d, c, dir), request, context);
    }

    @HillviewRpc
    public void findFiles(RpcRequest request, RpcRequestContext context) {
        FileSetDescription desc = request.parseArgs(FileSetDescription.class);
        HillviewLogger.instance.info("Finding files", "{0}", desc);
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap(desc);
        Converters.checkNull(this.emptyDataset);
        String folder = Utilities.getFolder(desc.fileNamePattern);

        String privacyMetadataFile = DPWrapper.privacyMetadataFile(folder);
        if (privacyMetadataFile != null) {
            this.runFlatMap(this.emptyDataset, finder,
                    (d, c) -> new PrivateFileDescriptionTarget(d, c, privacyMetadataFile), request, context);
        } else {
            this.runFlatMap(this.emptyDataset, finder,
                    (d, c) -> new FileDescriptionTarget(d, c, folder), request, context);
        }
    }

    @HillviewRpc
    public void findLogs(RpcRequest request, RpcRequestContext context) {
        @Nullable String cookie = request.parseArgs(String.class);
        FileSetDescription desc = new FileSetDescription();
        desc.cookie = cookie;
        desc.fileKind = "hillviewlog";
        desc.fileNamePattern = "./hillview*.log";
        desc.repeat = 1;
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap(desc);
        HillviewLogger.instance.info("Finding log files");
        assert this.emptyDataset != null;
        this.runFlatMap(this.emptyDataset, finder,
                (d, c) -> new FileDescriptionTarget(d, c, null), request, context);
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
    public void setMemoization(RpcRequest request, RpcRequestContext context) {
        SetMemoization tm = new SetMemoization(true);
        this.runManage(Converters.checkNull(this.emptyDataset), tm, request, context);
    }

    @HillviewRpc
    public void unsetMemoization(RpcRequest request, RpcRequestContext context) {
        SetMemoization tm = new SetMemoization(false);
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
        RpcServer.requestCompleted(request, session);
        request.syncCloseSession(session);
    }
}

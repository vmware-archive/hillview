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
import org.hillview.maps.FindDeltaTableFilesMap;
import org.hillview.storage.delta.DeltaTableDescription;
import org.hillview.storage.jdbc.JdbcConnectionInformation;
import org.hillview.storage.jdbc.JdbcDatabase;
import org.hillview.table.PrivacySchema;
import org.hillview.dataset.RemoteDataSet;
import org.hillview.dataset.api.*;
import org.hillview.dataset.remoting.HillviewServer;
import org.hillview.management.*;
import org.hillview.maps.FindFilesMap;
import org.hillview.maps.highorder.IdMap;
import org.hillview.maps.LoadDatabaseTableMap;
import org.hillview.storage.*;
import org.hillview.table.Schema;
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
        Converters.checkNull(this.emptyDataset);
        this.returnResult(Configuration.instance.getAsJson(), request, context);
    }

    @HillviewRpc
    public void openingBookmark(RpcRequest request, RpcRequestContext context) throws IOException {
        String bookmarkFile = request.parseArgs(String.class);
        File file = new File(InitialObjectTarget.bookmarkDirectory, bookmarkFile);
        String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        Converters.checkNull(this.emptyDataset);
        this.returnResult(new JsonInString(content), request, context);
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

    @SuppressWarnings("NotNullFieldNotInitialized")
    static class GreenplumInfo {
        FileSetDescription files;
        Schema schema;
        JdbcConnectionInformation jdbc;
    }

    @HillviewRpc
    public void findGreenplumFiles(RpcRequest request, RpcRequestContext context) {
        GreenplumInfo desc = request.parseArgs(GreenplumInfo.class);
        HillviewLogger.instance.info("Finding files", "{0}", desc);
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap<>(desc.files);
        String folder = Utilities.getFolder(desc.files.fileNamePattern);
        Converters.checkNull(desc.schema);
        Converters.checkNull(this.emptyDataset);
        JdbcDatabase database = new JdbcDatabase(desc.jdbc);
        String tmpTableName = Utilities.getBasename(folder);
        this.runFlatMap(this.emptyDataset, finder,
                (d, c) -> new GreenplumFileDescriptionTarget(
                        d, c, folder, tmpTableName, database, desc.schema), request, context);
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
    public void loadGreenplumTable(RpcRequest request, RpcRequestContext context) {
        // To load the data from greenplum we first use JDBC to connect to the
        // Greenplum root node and retrieve the metadata for the table.  This
        // path is similar to the simpleDB
        JdbcConnectionInformation conn = request.parseArgs(JdbcConnectionInformation.class);
        IMap<Empty, Empty> map = new IdMap<Empty>();
        Converters.checkNull(this.emptyDataset);
        String dir = Paths.get(Converters.checkNull(conn.databaseKind).toLowerCase(),
                Converters.checkNull(conn.database),
                conn.table).toString();
        this.runMap(this.emptyDataset, map, (e, c) -> new GreenplumStubTarget(conn, c, dir), request, context);
    }

    @HillviewRpc
    public void loadDeltaTable(RpcRequest request, RpcRequestContext context) {
        DeltaTableDescription desc = request.parseArgs(DeltaTableDescription.class);
        HillviewLogger.instance.info("Loading delta table", "{0}", desc);
        IMap<Empty, List<IFileReference>> finder = new FindDeltaTableFilesMap<>(desc);
        runFlatMap(emptyDataset, finder, (d, c) -> new FileDescriptionTarget(d, c, desc.path), request, context);
    }

    @HillviewRpc
    public void findFiles(RpcRequest request, RpcRequestContext context) {
        FileSetDescription desc = request.parseArgs(FileSetDescription.class);
        HillviewLogger.instance.info("Finding files", "{0}", desc);
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap<>(desc);
        Converters.checkNull(this.emptyDataset);
        String folder;
        if (desc.fileKind.equals("lazycsv"))
            // These files are generated from a database, use the database/table name,
            // which is now in the 'name' field of the FileSetDescription
            folder = desc.name;
        else
            folder = Utilities.getFolder(desc.fileNamePattern);

        String privacyMetadataFile = DPWrapper.privacyMetadataFile(Converters.checkNull(folder));
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
        IMap<Empty, List<IFileReference>> finder = new FindFilesMap<>(desc);
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

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

import {DatasetView, IDatasetSerialization} from "./datasetView";
import {SchemaReceiver, TableTargetAPI} from "./modules";
import {
    FileSetDescription,
    FileSizeSketchInfo,
    JdbcConnectionInformation,
    CassandraConnectionInfo,
    RemoteObjectId, FederatedDatabase, TableSummary,
} from "./javaBridge";
import {OnCompleteReceiver, RemoteObject} from "./rpc";
import {BaseReceiver} from "./tableTarget";
import {FullPage, PageTitle} from "./ui/fullPage";
import {ICancellable, significantDigits, getUUID, assertNever} from "./util";

export interface FilesLoaded {
    kind: "Files";
    description: FileSetDescription;
}

export interface TablesLoaded {
    kind: "DB";
    description: JdbcConnectionInformation;
}

export interface HillviewLogs {
    kind: "Hillview logs";
}

export interface SSTableFilesLoaded {
    kind: "SSTable";
    description: CassandraConnectionInfo;
}

export type DataLoaded = FilesLoaded | TablesLoaded | HillviewLogs | IDatasetSerialization | SSTableFilesLoaded;

export function getDescription(data: DataLoaded): PageTitle {
    switch (data.kind) {
        case "Saved dataset":
            return new PageTitle("saved", "");
        case "Files":
            if (data.description.name != null)
                return new PageTitle(data.description.name, "loaded from files");
            else
                return new PageTitle(data.description.fileNamePattern, "loaded from files");
        case "DB":
            return new PageTitle(data.description.database + "/" + data.description.table,
                "loaded from database");
        case "Hillview logs":
            return new PageTitle("logs", "Hillview installation logs");
        case "SSTable":
            return new PageTitle(data.description.database + "/" + data.description.table, "loaded from files");
    }
}

/**
 * A renderer which receives a remote object id that denotes a set of files.
 * Initiates an RPC to get the file size.
 */
class FilesReceiver extends OnCompleteReceiver<RemoteObjectId> {
    constructor(sourcePage: FullPage, operation: ICancellable<RemoteObjectId>,
                protected data: DataLoaded,
                protected newDataset: boolean) {
        super(sourcePage, operation, "Discovering files on disk");
    }

    public run(remoteObjId: RemoteObjectId): void {
        const fn = new RemoteObject(remoteObjId);
        const rr = fn.createStreamingRpcRequest<FileSizeSketchInfo>("getFileSize", null);
        rr.chain(this.operation);
        const observer = new FileSizeReceiver(this.page, rr, this.data, fn, this.newDataset);
        rr.invoke(observer);
    }
}

/**
 * Receives the file size.
 * It initiates a loadTable RPC request to load data from these files as a table.
 */
class FileSizeReceiver extends OnCompleteReceiver<FileSizeSketchInfo> {
    constructor(sourcePage: FullPage, operation: ICancellable<FileSizeSketchInfo>,
                protected data: DataLoaded,
                protected remoteObj: RemoteObject,
                protected newDataset: boolean) {
        super(sourcePage, operation, "Enumerating files");
    }

    public run(size: FileSizeSketchInfo): void {
        if (size.fileCount === 0) {
            this.page.reportError("No files matching " + getDescription(this.data).format);
            return;
        }

        if (false) {
            // Prune the dataset; may increase efficiency
            // TODO: prune seems to be broken.
            const rr = this.remoteObj.createStreamingRpcRequest<RemoteObjectId>("prune", null);
            rr.chain(this.operation);
            const observer = new FilePruneReceiver(this.page, rr, this.data, size, this.newDataset);
            rr.invoke(observer);
        } else {
            const fileSize = "Loading " + size.fileCount + " file(s), total size " +
                significantDigits(size.totalSize) + " bytes";
            const fn = new RemoteObject(this.remoteObj.remoteObjectId);
            const rr = fn.createStreamingRpcRequest<RemoteObjectId>("loadTable", null);
            rr.chain(this.operation);
            const observer = new RemoteTableReceiver(this.page, rr, this.data, fileSize, this.newDataset);
            rr.invoke(observer);
        }
    }
}

class FilePruneReceiver extends OnCompleteReceiver<RemoteObjectId> {
    constructor(sourcePage: FullPage, operation: ICancellable<RemoteObjectId>,
                protected data: DataLoaded, protected readonly size: FileSizeSketchInfo,
                protected newDataset: boolean) {
        super(sourcePage, operation, "Disconnecting workers without data");
    }

    public run(remoteObjId: RemoteObjectId): void {
        const fileSize = "Loading " + this.size.fileCount + " file(s), total size " +
            significantDigits(this.size.totalSize) + " bytes";
        const fn = new RemoteObject(remoteObjId);
        const rr = fn.createStreamingRpcRequest<RemoteObjectId>("loadTable", null);
        rr.chain(this.operation);
        const observer = new RemoteTableReceiver(this.page, rr, this.data, fileSize, this.newDataset);
        rr.invoke(observer);
    }
}

/**
 * Receives the ID for a remote table and initiates a request to get the
 * table schema.
 */
class RemoteTableReceiver extends BaseReceiver {
    /**
     * Create a renderer for a new table.
     * @param sourcePage      Parent page initiating this request.
     * @param data            Data that has been loaded.
     * @param operation       Operation that will bring the results.
     * @param description     Description of the files that are being loaded.
     * @param newDataset      If true this is a new dataset.
     */
    constructor(sourcePage: FullPage, operation: ICancellable<RemoteObjectId>, protected data: DataLoaded,
                description: string, protected newDataset: boolean) {
        super(sourcePage, operation, description, null);
    }

    public run(value: RemoteObjectId): void {
        super.run(value);
        const rr = this.remoteObject.createGetSummaryRequest();
        rr.chain(this.operation);
        const title = getDescription(this.data);
        if (this.newDataset) {
            const dataset = new DatasetView(this.remoteObject.remoteObjectId, title.format, this.data, this.page);
            const newPage = dataset.newPage(title, null);
            rr.invoke(new SchemaReceiver(newPage, rr, this.remoteObject, dataset, null, null));
        } else {
            rr.invoke(new SchemaReceiver(this.page, rr, this.remoteObject, this.page.dataset!, null, null));
        }
    }
}

/**
 * Receives the ID for a remote GreenplumTarget and initiates a request to get the
 * table schema.
 */
class GreenplumTableReceiver extends BaseReceiver {
    /**
     * Create a renderer for a new table.
     * @param loadMenuPage    Parent page initiating this request, always the page of the LoadMenu.
     * @param data            Data that has been loaded.
     * @param initialObject   Handle to the initial object; used later to load the files
     *                        obtained from dumping the table.
     * @param operation       Operation that will bring the results.
     */
    constructor(loadMenuPage: FullPage, operation: ICancellable<RemoteObjectId>,
                protected initialObject: InitialObject,
                protected data: TablesLoaded) {
        super(loadMenuPage, operation, "Connecting to Greenplum database", null);
    }

    public run(value: RemoteObjectId): void {
        super.run(value);
        const rr = this.remoteObject.createGetSummaryRequest();
        rr.chain(this.operation);
        const title = getDescription(this.data);
        const dataset = new DatasetView(this.remoteObject.remoteObjectId, title.format, this.data, this.page);
        const newPage = dataset.newPage(title, null);
        rr.invoke(new GreenplumSchemaReceiver(
            newPage, rr, this.initialObject, this.remoteObject, this.data.description));
    }
}

class GreenplumSchemaReceiver extends OnCompleteReceiver<TableSummary> {
    constructor(page: FullPage, operation: ICancellable<TableSummary>,
                protected initialObject: InitialObject,
                protected remoteObject: TableTargetAPI,
                protected jdbc: JdbcConnectionInformation) {
        super(page, operation, "Reading table metadata");
    }

    public run(ts: TableSummary): void {
        if (ts.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }
        // Ask Greenplum to dump the data; receive back the name of the temporary files
        // where the tables are stored on the remote machines.
        // This is the name of the temporary table used.
        const tableName = "T" + getUUID().replace(/-/g, '');
        const rr = this.remoteObject.createStreamingRpcRequest<string>("initializeTable", tableName);
        rr.chain(this.operation);
        rr.invoke(new GreenplumLoader(this.page, ts, this.initialObject, this.jdbc, rr));
    }
}

class GreenplumLoader extends OnCompleteReceiver<string> {
    constructor(page: FullPage, protected summary: TableSummary,
                protected remoteObject: InitialObject,
                protected jdbc: JdbcConnectionInformation,
                operation: ICancellable<string>) {
        super(page, operation, "Dumping data from database");
    }

    public run(value: string): void {
        /*
        This was an attempt to only load one column from greenplum, but it
        does not seem to work.

        const files: FileSetDescription = {
            fileKind: "lazycsv",
            fileNamePattern: value,
            schemaFile: null,
            headerRow: false,
            schema: this.summary.schema,
            name: (this.page.dataset?.loaded as TablesLoaded).description.table,
            deleteAfterLoading: true,
        };
        const rr = this.remoteObject.createStreamingRpcRequest<RemoteObjectId>(
            "findGreenplumFiles", {
                files: files,
                schema: this.summary.schema,
                jdbc: this.jdbc
            });
         */
        const files: FileSetDescription = {
            fileKind: "csv",
            fileNamePattern: value,
            schemaFile: null,
            headerRow: false,
            schema: this.summary.schema,
            name: (this.page.dataset?.loaded as TablesLoaded).description.table,
            deleteAfterLoading: true,
        };
        const rr = this.remoteObject.createStreamingRpcRequest<RemoteObjectId>(
            "findFiles", files);
        rr.chain(this.operation);
        const observer = new FilesReceiver(this.page, rr,
            { kind: "Files", description: files }, false);
        rr.invoke(observer);
    }
}

/**
 * This is the first object created that refers to a remote object.
 */
export class InitialObject extends RemoteObject {
    public static instance: InitialObject = new InitialObject();

    // noinspection JSUnusedLocalSymbols
    private constructor() {
        // The "-1" argument is the object id for the initial object.
        // It must match the id of the object declared in RpcServer.java.
        // This is a "well-known" name used for bootstrapping the system.
        // noinspection JSUnusedLocalSymbols
        super("-1");
    }

    public loadFiles(files: FileSetDescription, loadMenuPage: FullPage): void {
        const rr = this.createStreamingRpcRequest<RemoteObjectId>("findFiles", files);
        const observer = new FilesReceiver(loadMenuPage, rr,
            { kind: "Files", description: files }, true);
        rr.invoke(observer);
    }

    public loadCassandraFiles(conn: CassandraConnectionInfo, loadMenuPage: FullPage): void {
        const rr = this.createStreamingRpcRequest<RemoteObjectId>("findCassandraFiles", conn);
        const observer = new FilesReceiver(loadMenuPage, rr,
            { kind: "SSTable", description: conn }, true);
        rr.invoke(observer);
    }

    public loadLogs(loadMenuPage: FullPage): void {
        // Use a guid to force the request to reload every time
        const rr = this.createStreamingRpcRequest<RemoteObjectId>("findLogs", getUUID());``
        const observer = new FilesReceiver(loadMenuPage, rr, { kind: "Hillview logs"}, true);
        rr.invoke(observer);
    }

    protected loadTable(conn: JdbcConnectionInformation, loadMenuPage: FullPage, method: string): void {
        const rr = this.createStreamingRpcRequest<RemoteObjectId>(method, conn);
        const observer = new RemoteTableReceiver(loadMenuPage, rr,
            { kind: "DB", description: conn }, "loading database table", true);
        rr.invoke(observer);
    }

    public loadSimpleDBTable(conn: JdbcConnectionInformation, loadMenuPage: FullPage): void {
        this.loadTable(conn, loadMenuPage, "loadSimpleDBTable");
    }

    protected loadGreenplumTable(conn: JdbcConnectionInformation, loadMenuPage: FullPage, method: string): void {
        const rr = this.createStreamingRpcRequest<RemoteObjectId>(method, conn);
        const observer = new GreenplumTableReceiver(loadMenuPage, rr, this,
            { kind: "DB", description: conn });
        rr.invoke(observer);
    }

    public loadFederatedDBTable(conn: JdbcConnectionInformation | CassandraConnectionInfo | null,
            db: FederatedDatabase | null, loadMenuPage: FullPage): void {
        if (db == null || conn == null) {
            loadMenuPage.reportError("Unknown database kind");
            return;
        }
        switch (db) {
            case "mysql":
            case "impala":
                this.loadTable(conn as JdbcConnectionInformation, loadMenuPage, "loadDBTable");
                break;
            case "cassandra":
                this.loadCassandraFiles(conn as CassandraConnectionInfo, loadMenuPage);
                break;
            case "greenplum":
                this.loadGreenplumTable(conn as JdbcConnectionInformation, loadMenuPage, "loadGreenplumTable");
                break;
            default:
                assertNever(db);
        }
    }
}

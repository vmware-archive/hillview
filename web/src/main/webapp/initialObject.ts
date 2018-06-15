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

import {RemoteObject, OnCompleteReceiver} from "./rpc";
import {FullPage} from "./ui/fullPage";
import {ICancellable, significantDigits, uuidv4} from "./util";
import {
    FileSetDescription,
    FileSizeSketchInfo,
    JdbcConnectionInformation,
    RemoteObjectId
} from "./javaBridge";
import {DatasetView, IDatasetSerialization} from "./datasetView";
import {BaseRenderer} from "./tableTarget";
import {SchemaReceiver} from "./dataViews/tableView";

export interface FilesLoaded {
    kind: "Files",
    description: FileSetDescription
}

export interface TablesLoaded {
    kind: "DB",
    description: JdbcConnectionInformation
}

export interface HillviewLogs {
    kind: "Hillview logs",
}

export type DataLoaded = FilesLoaded | TablesLoaded | HillviewLogs | IDatasetSerialization;

export function getDescription(data: DataLoaded): string {
    switch (data.kind) {
        case "Saved dataset":
            return "saved";
        case "Files":
            if (data.description.name != null)
                return data.description.name;
            else
                return data.description.folder + "/" + data.description.fileNamePattern;
        case "DB":
            return data.description.database + "/" + data.description.table;
        case "Hillview logs":
            return "logs";
    }
}

/**
 * A renderer which receives a remote object id that denotes a set of files.
 * Initiates an RPC to get the file size.
 */
class FileNamesReceiver extends OnCompleteReceiver<RemoteObjectId> {
    constructor(loadMenuPage: FullPage, operation: ICancellable, protected data: DataLoaded) {
        super(loadMenuPage, operation, "Get file info");
    }

    public run(remoteObjId: RemoteObjectId): void {
        let fn = new RemoteObject(remoteObjId);
        let rr = fn.createStreamingRpcRequest<FileSizeSketchInfo>("getFileSize", null);
        rr.chain(this.operation);
        let observer = new FileSizeReceiver(this.page, rr, this.data, fn);
        rr.invoke(observer);
    }
}

/**
 * Receives the file size.
 * It initiates a loadTable RPC request to load data from these files as a table.
 */
class FileSizeReceiver extends OnCompleteReceiver<FileSizeSketchInfo> {
    constructor(loadMenuPage: FullPage, operation: ICancellable,
                protected data: DataLoaded,
                protected remoteObj: RemoteObject) {
        super(loadMenuPage, operation, "Load data");
    }

    public run(size: FileSizeSketchInfo): void {
        if (size.fileCount == 0) {
            this.page.reportError("No files matching " + getDescription(this.data));
            return;
        }
        let fileSize = "Loading " + size.fileCount + " file(s), total size " +
            significantDigits(size.totalSize);
        let rr = this.remoteObj.createStreamingRpcRequest<RemoteObjectId>("loadTable", null);
        rr.chain(this.operation);
        let observer = new RemoteTableReceiver(this.page, rr, this.data, fileSize, false);
        rr.invoke(observer);
    }
}

/**
 * Receives the ID for a remote table and initiates a request to get the
 * table schema.
 */
export class RemoteTableReceiver extends BaseRenderer {
    /**
     * Create a renderer for a new table.
     * @param page            Parent page initiating this request.
     * @param data            Data that has been loaded.
     * @param operation       Operation that will bring the results.
     * @param progressInfo    Description of the files that are being loaded.
     * @param forceTableView  If true the resulting view is always a table.
     */
    constructor(page: FullPage, operation: ICancellable, protected data: DataLoaded,
                progressInfo: string, protected forceTableView: boolean) {
        super(page, operation, progressInfo, null);
    }

    public run(): void {
        super.run();
        let rr = this.remoteObject.createGetSchemaRequest();
        rr.chain(this.operation);
        let title = getDescription(this.data);
        let dataset = new DatasetView(this.remoteObject.remoteObjectId, title, this.data);
        let newPage = dataset.newPage(title, null);
        rr.invoke(new SchemaReceiver(newPage, rr, this.remoteObject, dataset, this.forceTableView));
    }
}

/**
 * This is the first object created that refers to a remote object.
 */
export class InitialObject extends RemoteObject {
    public static instance: InitialObject = new InitialObject();

    // noinspection JSUnusedLocalSymbols
    private constructor() {
        // The "0" argument is the object id for the initial object.
        // It must match the id of the object declared in RpcServer.java.
        // This is a "well-known" name used for bootstrapping the system.
        // noinspection JSUnusedLocalSymbols
        super("0");
    }

    public loadFiles(files: FileSetDescription, loadMenuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("findFiles", files);
        let observer = new FileNamesReceiver(loadMenuPage, rr,
            { kind: "Files", description: files });
        rr.invoke(observer);
    }

    public loadLogs(loadMenuPage: FullPage): void {
        // Use a guid to force the request to reload every time
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("findLogs", uuidv4());
        let observer = new FileNamesReceiver(loadMenuPage, rr, { kind: "Hillview logs"} );
        rr.invoke(observer);
    }

    public loadDBTable(conn: JdbcConnectionInformation, loadMenuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("loadDBTable", conn);
        let observer = new RemoteTableReceiver(loadMenuPage, rr,
            { kind: "DB", description: conn }, "loading database table", false);
        rr.invoke(observer);
    }
}

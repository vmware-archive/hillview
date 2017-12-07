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

import {RemoteObject, OnCompleteRenderer} from "./rpc";
import {RemoteTableReceiver} from "./dataViews/tableView";
import {FullPage} from "./ui/fullPage";
import {ICancellable} from "./util";
import {FileSetDescription, JdbcConnectionInformation, RemoteObjectId} from "./javaBridge";

/**
 * A renderer which receives a remote object id that denotes a set of files.
 * It initiates a loadTable RPC request to load data from these files as a table.
 */
class FileNamesReceiver extends OnCompleteRenderer<RemoteObjectId> {
    constructor(page: FullPage, operation: ICancellable, protected title: string) {
        super(page, operation, "Load files");
    }

    public run(remoteObjId: RemoteObjectId): void {
        let fn = new RemoteObject(remoteObjId);
        let rr = fn.createStreamingRpcRequest<RemoteObjectId>("loadTable", null);
        rr.chain(this.operation);
        let observer = new RemoteTableReceiver(this.page, rr, this.title, false, null);
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
        // The "0" argument is the object id for the initial object.
        // It must match the id of the object declared in RpcServer.java.
        // This is a "well-known" name used for bootstrapping the system.
        // noinspection JSUnusedLocalSymbols
        super("0");
    }

    public testDataset(which: number, menuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("testDataset", which);
        let observer = new FileNamesReceiver(menuPage, rr, "Test data set");
        rr.invoke(observer);
    }

    public loadCSVFiles(files: FileSetDescription, menuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("findCsvFiles", files);
        let observer = new FileNamesReceiver(menuPage, rr, files.fileNamePattern);
        rr.invoke(observer);
    }

    public loadLogs(menuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("findLogs", null);
        let observer = new FileNamesReceiver(menuPage, rr, "Hillview logs");
        rr.invoke(observer);
    }

    public loadJsonFiles(files: FileSetDescription, menuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("findJsonFiles", files);
        let observer = new FileNamesReceiver(menuPage, rr, files.fileNamePattern);
        rr.invoke(observer);
    }

    public loadDBTable(conn: JdbcConnectionInformation, menuPage: FullPage): void {
        let rr = this.createStreamingRpcRequest<RemoteObjectId>("loadDBTable", conn);
        let title = "DB " + conn.database + ":" + conn.table;
        let observer = new RemoteTableReceiver(menuPage, rr, title, false, null);
        rr.invoke(observer);
    }
}

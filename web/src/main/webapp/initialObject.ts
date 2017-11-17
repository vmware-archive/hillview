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
import {CSVFilesDescription, JdbcConnectionInformation} from "./javaBridge";

class FileNamesReceiver extends OnCompleteRenderer<string> {
    constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "Find files");
    }

    public run(remoteObjId: string): void {
        let fn = new RemoteObject(remoteObjId);
        let rr = fn.createRpcRequest("loadTable", null);
        rr.chain(this.operation);
        let observer = new RemoteTableReceiver(this.page, rr);
        rr.invoke(observer);
    }
}

/**
 * This is the first object created that refers to a remote object.
 */
export class InitialObject extends RemoteObject {
    public static instance: InitialObject = new InitialObject();

    private constructor() {
        // The "0" argument is the object id for the initial object.
        // It must match the id of the object declared in RpcServer.java.
        // This is a "well-known" name used for bootstrapping the system.
        // noinspection JSUnusedLocalSymbols
        super("0");
    }

    public testDataset(which: number, menuPage: FullPage): void {
        let rr = this.createRpcRequest("testDataset", which);
        let page = new FullPage("Test dataset", "Table", null);
        page.append();
        let observer = new FileNamesReceiver(page, rr);
        rr.invoke(observer);
    }

    public loadCSVFiles(files: CSVFilesDescription, menuPage: FullPage): void {
        let rr = this.createRpcRequest("findCSVFiles", files);
        let page = new FullPage(files.fileNamePattern, "Table", null);
        page.append();
        let observer = new FileNamesReceiver(page, rr);
        rr.invoke(observer);
    }

    public loadLogs(menuPage: FullPage): void {
        let rr = this.createRpcRequest("findLogs", null);
        let page = new FullPage("Hillview logs", "Table", null);
        page.append();
        let observer = new LogFileReceiver(page, rr);
        rr.invoke(observer);
    }

    public loadDBTable(conn: JdbcConnectionInformation, menuPage: FullPage): void {
        let rr = this.createRpcRequest("loadDBTable", conn);
        let page = new FullPage("DB " + conn.database + ":" + conn.table, "Table", null);
        page.append();
        let observer = new RemoteTableReceiver(page, rr);
        rr.invoke(observer);
    }
}

/**
 * Receives and displays the Hillview system logs as a tabular view.
 */
class LogFileReceiver extends OnCompleteRenderer<string> {
    constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "Find logs");
    }

    public run(objId: string): void {
        let fn = new RemoteObject(objId);
        let rr = fn.createRpcRequest("loadTable", null);
        let observer = new RemoteTableReceiver(this.page, rr);
        rr.invoke(observer);
    }
}

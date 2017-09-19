/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {RemoteObject, Renderer} from "./rpc";
import {RemoteTableReceiver} from "./table";
import {FullPage} from "./ui";
import {ICancellable, PartialResult} from "./util";

class FileNames extends RemoteObject {
    constructor(remoteObjectId: string) {
        super(remoteObjectId);
    }

    public loadFiles(page: FullPage, operation: ICancellable): void {
        let rr = this.createRpcRequest("loadTable", null);
        rr.chain(operation);
        let observer = new RemoteTableReceiver(page, rr);
        rr.invoke(observer);
    }
}

class FileNamesReceiver extends Renderer<string> {
    private remoteObjId: string;

    constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "Find files");
    }

    // only one reply expected
    public onNext(value: PartialResult<string>): void {
        super.onNext(value);
        if (value.data != null)
            this.remoteObjId = value.data;
    }

    public onCompleted(): void {
        this.finished();
        if (this.remoteObjId == null)
            return;

        let fn = new FileNames(this.remoteObjId);
        fn.loadFiles(this.page, this.operation);
    }
}

export class InitialObject extends RemoteObject {
    public static instance: InitialObject = new InitialObject();

    // The "0" argument is the object id for the initial object.
    // It must match the id of the object declared in RpcServer.java.
    // This is a "well-known" name used for bootstrapping the system.
    private constructor() { super("0"); }

    public loadFiles(which: number): void {
        let rr = this.createRpcRequest("prepareFiles", which);
        let page = new FullPage();
        page.append();
        let observer = new FileNamesReceiver(page, rr);
        rr.invoke(observer);
    }

    public loadDBTable(): void {
        let rr = this.createRpcRequest("loadDBTable", null);
        let page = new FullPage();
        page.append();
        let observer = new RemoteTableReceiver(page, rr);
        rr.invoke(observer);
    }
}

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

import {RemoteObject, ICancellable, RpcReceiver, PartialResult} from "./rpc";
import {RemoteTableReceiver} from "./table";
import {FullPage, Renderer} from "./ui";

class FileNames extends RemoteObject {
    constructor(remoteObjectId: string) {
        super(remoteObjectId);
    }

    public loadTable(page: FullPage, startTime: Date): void {
        let rr = this.createRpcRequest("loadTable", null);
        rr.setStartTime(startTime);
        let observer = new RemoteTableReceiver(page, rr);
        rr.invoke(observer);
    }
}

class FileNamesReceiver extends Renderer<string> {
    private files: FileNames;

    constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "Find files");
    }

    // only one reply expected
    public onNext(value: PartialResult<string>): void {
        super.onNext(value);
        if (value.data != null)
            this.files = new FileNames(value.data);
    }

    public onCompleted(): void {
        this.finished();
        if (this.files)
            this.files.loadTable(this.page, this.operation.startTime());
    }
}

export class InitialObject extends RemoteObject {
    public static instance: InitialObject = new InitialObject();

    // The "0" argument is the object id for the initial object.
    // It must match the id of the object declared in RpcServer.java.
    // This is a "well-known" name used for bootstrapping the system.
    private constructor() { super("0"); }

    public loadTable(which: number): void {
        let rr = this.createRpcRequest("prepareFiles", which);
        let page = new FullPage();
        page.append();
        let observer = new FileNamesReceiver(page, rr);
        rr.invoke(observer);
    }
}

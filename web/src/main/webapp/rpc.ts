/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// <reference path="node_modules/rx/ts/rx.d.ts" />

import Rx = require('rx');
import Observer = Rx.Observer;
import Observable = Rx.Observable;
import d3 = require('d3');
import {ErrorReporter, ConsoleErrorReporter} from "./errReporter";
import {ProgressBar, FullPage} from "./ui";
import {PartialResult, ICancellable, EnumIterators, RpcReply} from "./util";
import {TopSubMenu} from "./menu";

// path in server url for rpc web sockets
const RpcRequestPath = "rpc";

export class RemoteObject {
    constructor(public readonly remoteObjectId : string) {}

    createRpcRequest(method: string, args: any) : RpcRequest {
        return new RpcRequest(this.remoteObjectId, method, args);
    }

    selectCurrent(): void {
        SelectedObject.current.select(this);
    }

    // Combines the current RemoteObject with the currently
    // selected object (SelectedObject.current.getSelected)
    // according to the specified operation.  SHould be overridden
    // in subclasses.
    combine(how: CombineOperators): void {}

    public toString(): string {
        return this.remoteObjectId;
    }
}

// A streaming RPC request: for each request made
// we expect a stream of replies.  The requests are made
// over web sockets.  When the last reply has been received
// the web socket is closed.
export class RpcRequest implements ICancellable {
    readonly protoVersion : number = 6;
    readonly requestId: number;
    cancelled: boolean;
    closed:    boolean;  // i.e., not opened
    socket:    WebSocket;
    rpcTime: Date; /* Time when RPC was initiated.  It may be set explicitly
                      by users, and then it can be used to measured operations
                      that span multiple RPCs */

    static requestCounter : number = 0;

    constructor(public objectId : string,
                public method : string,
                public args : any) {
        this.requestId = RpcRequest.requestCounter++;
        this.socket = null;
        this.cancelled = false;
        this.closed = true;
        this.rpcTime = null;
    }

    serialize() : string {
        let argString = "";
        if (this.args == null)
            argString = JSON.stringify(null);
        else if  (this.args.toJSON != null)
            argString = this.args.toJSON();
        else
            argString = JSON.stringify(this.args);
        let result = {
            "objectId": this.objectId,
            "method": this.method,
            "arguments": argString,
            "requestId": this.requestId,
            "protoVersion": this.protoVersion
        };
        return JSON.stringify(result);
    }

    public startTime(): Date {
        return this.rpcTime;
    }

    public setStartTime(start: Date): void{
        this.rpcTime = start;
    }

    // Indicates that this rpc request is executed as a continuation of
    // the specified operation.
    public chain(after: ICancellable) {
        if (after != null)
            this.setStartTime(after.startTime());
    }

    public cancel(): boolean {
        if (!this.closed) {
            this.closed = true;
            this.socket.close();
            return true;
        }
        return false;
    }

    // Function to call to execute the RPC.
    // onReply is an observer which is invoked for
    // each result received by the streaming RPC.
    public invoke<T>(onReply : Observer<T>) : void {
        try {
            // Create a web socked and send the request
            if (this.rpcTime == null)
                this.rpcTime = new Date();
            let rpcRequestUrl = "ws://" + window.location.hostname + ":" + window.location.port + "/" + RpcRequestPath;
            this.socket = new WebSocket(rpcRequestUrl);
            this.socket.onerror = function (ev: ErrorEvent) {
                console.log("socket error " + ev);
                let msg = ev.message;
                if (msg == null)
                    msg = "Error communicating to server.";
                onReply.onError(msg);
            };
            this.socket.onmessage = function (r: MessageEvent) {
                // parse json and invoke onReply.onNext
                console.log('reply received: ' + r.data);
                let reply = <RpcReply>JSON.parse(r.data);
                if (reply.isError) {
                    onReply.onError(reply.result);
                } else {
                    let success = false;
                    let response: any;
                    try {
                        response = <T>JSON.parse(reply.result);
                        success = true;
                    } catch (e) {
                        onReply.onError(e);
                    }
                    if (success)
                        onReply.onNext(response);
                }
            };
            this.socket.onopen = () => {
                this.closed = false;
                let reqStr: string = this.serialize();
                console.log("Sending message " + reqStr);
                this.socket.send(reqStr);
            };
            this.socket.onclose = (e: CloseEvent) => {
                console.log("Socket closed");
                if (e.code == 1000) {
                    onReply.onCompleted();
                    return; // normal
                }

                let reason = "Unknown reason.";
                // See http://tools.ietf.org/html/rfc6455#section-7.4.1
                if (e.code == 1001)
                    reason = "Endpoint disconnected.";
                else if (e.code == 1002)
                    reason = "Protocol error.";
                else if (e.code == 1003)
                    reason = "Incorrect data.";
                else if (e.code == 1004)
                    reason = "Reserved.";
                else if (e.code == 1005)
                    reason = "No status code.";
                else if (e.code == 1006)
                    reason = "Connection closed abnormally.";
                else if (e.code == 1007)
                    reason = "Incorrect message type.";
                else if (e.code == 1008)
                    reason = "Message violates policy.";
                else if (e.code == 1009)
                    reason = "Message too large.";
                else if (e.code == 1010)
                    reason = "Protocol extension not supported.";
                else if (e.code == 1011)
                    reason = "Unexpected server condition.";
                else if (e.code == 1015)
                    reason = "Cannot verify server TLS certificate.";
                // else unknown
                onReply.onError(reason);
                onReply.onCompleted();
            }
        } catch (e) {
            onReply.onError(e);
        }
    };
}

export abstract class RpcReceiver<T> implements Rx.Observer<T> {
    public constructor(protected progressBar: ProgressBar,
                       protected reporter: ErrorReporter) {
        if (this.reporter == null)
            this.reporter = ConsoleErrorReporter.instance;
    }

    //noinspection JSUnusedLocalSymbols
    public makeSafe(disposable: Rx.IDisposable): Rx.Observer<T> {
        return null;
    }

    public finished(): void {
        this.progressBar.setFinished();
    }

    public abstract onNext(value: T): void;

    public onError(exception: any): void {
        this.reporter.reportError(String(exception));
        this.finished();
    }

    public onCompleted(): void { this.finished(); }
}

// Used for operations between multiple objects: the selected object
// is a RemoteObject which can be combined with another one.
export class SelectedObject {
    private selected: RemoteObject;

    private constructor() {
        this.selected = null;
    }

    select(object: RemoteObject) {
        this.selected = object;
    }

    getSelected(): RemoteObject {
        return this.selected;
    }

    static current: SelectedObject = new SelectedObject();
}

export enum CombineOperators {
    Union, Intersection, Exclude, Replace
}

export function combineMenu(ro: RemoteObject): TopSubMenu {
    let combineMenu = [];
    combineMenu.push({
        text: "Select current",
        action: () => { SelectedObject.current.select(ro); }});
    combineMenu.push({text: "---", action: null});
    EnumIterators.getNamesAndValues(CombineOperators)
        .forEach(c => combineMenu.push({
            text: c.name,
            action: () => { ro.combine(c.value); } }));
    return new TopSubMenu(combineMenu);
}

export abstract class Renderer<T> extends RpcReceiver<PartialResult<T>> {
    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string) {
        super(page.progressManager.newProgressBar(operation, description),
            page.getErrorReporter());
        // TODO: This may be too eager.
        page.getErrorReporter().clear();
    }

    public onNext(value: PartialResult<T>) {
        this.progressBar.setPosition(value.done);
    }

    public elapsedMilliseconds(): number {
        return d3.timeMillisecond.count(this.operation.startTime(), new Date());
    }
}

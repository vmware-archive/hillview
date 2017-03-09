/// <reference path="node_modules/rx/ts/rx.d.ts" />
/// <reference path="typings/index.d.ts" />

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

import Rx = require('rx');
import RxDOM = require('rx-dom');
import Observer = Rx.Observer;
import Observable = Rx.Observable;
import {ErrorReporter, ConsoleErrorReporter} from "./errorReporter";
import {ProgressBar} from "./ui";

const HieroServiceUrl : string = "ws://localhost:8080";
const RpcRequestUrl = HieroServiceUrl + "/rpc";

/*
export interface IJSON {
    // Convert object to JSON
    toJSON(): string;
}
*/

export class RemoteObject {
    constructor(public readonly remoteObjectId : string) {}

    createRpcRequest(method: string, args: any) : RpcRequest {
        return new RpcRequest(this.remoteObjectId, method, args);
    }
}

export class PartialResult<T> {
    constructor(public done: number, public data: T) {}
}

export interface RpcReply {
    result: string;  // JSON or error message
    requestId: number;  // request that is being replied
    isError: boolean;
}

export interface ICancellable {
    // return 'true' if cancellation succeeds.
    // Cancellation may fail if the computation is terminated.
    cancel(): boolean;
}

// A streaming RPC request: for each request made
// we expect a stream of replies.  The requests are made
// over web sockets.  When the last reply has been received
// the web socked is closed.
export class RpcRequest implements ICancellable {
    readonly protoVersion : number = 6;
    readonly requestId: number;
    cancelled: boolean;
    closed:    boolean;  // i.e., not opened
    socket:    any; // result of Rx.DOM.fromWebSocket.
    // Should be Rx.Subject<MessageEvent>, but this does not typecheck
    // the this.socket.onNext method with a String argument.

    static requestCounter : number = 0;

    constructor(public objectId : string,
                public method : string,
                public args : any) {
        this.requestId = RpcRequest.requestCounter++;
        this.socket = null;
        this.cancelled = false;
        this.closed = true;
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

    private onOpen() : void {
        this.closed = false;
        console.log('socket open');
        let reqStr : string = this.serialize();
        console.log("Sending message " + reqStr);
        this.socket.onNext(reqStr);
    }

    public cancel(): boolean {
        if (!this.closed) {
            this.socket.onCompleted();
            return true;
        }
        return false;
    }

    // Function to call to execute the RPC.
    // onReply is an observer which is invoked for
    // each result received by the streaming RPC.
    public invoke<T>(onReply : Observer<T>) : void {
        // Invoked when the socked is opened
        let openObserver = Rx.Observer.create(() => this.onOpen());
        let closeObserver = Rx.Observer.create(
            function() { this.closed = true; console.log('socket closing'); },
            function(e: CloseEvent) {
                if (e == null)
                    return;

                // TODO: the onError method does not seem to be ever called.
                let reason = "Unknown reason";
                // See http://tools.ietf.org/html/rfc6455#section-7.4.1
                if (e.code == 1000)
                    return; // normal
                else if (e.code == 1001)
                    reason = "Endpoint disconnected.";
                else if (e.code == 1002)
                    reason = "Protocol error";
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
            },
            function() {}
        );

        try {
            // Create a web socked and send the request
            this.socket = RxDOM.DOM.fromWebSocket(RpcRequestUrl, null, openObserver, closeObserver);
            this.socket.onclose =
            console.log('socket created');
            this.socket.subscribe(
                // onNext -> parse json and invoke onReply.onNext
                function (r: MessageEvent) {
                    console.log('reply received: ' + r.data);
                    let reply = <RpcReply>JSON.parse(r.data);
                    if (reply.isError) {
                        onReply.onError(reply.result);
                    } else {
                        try {
                            let response = <T>JSON.parse(reply.result);
                            onReply.onNext(response);
                        } catch (e) {
                            onReply.onError(e);
                        }
                    }
                },
                // onError -> invoke onReply.onError
                function (e: MessageEvent) { onReply.onError(e.type); },
                // onCompleted -> invoke onReply.onCompleted
                function () { onReply.onCompleted(); }
            );
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

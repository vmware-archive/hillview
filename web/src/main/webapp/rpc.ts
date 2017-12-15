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

import {d3} from "./ui/d3-modules";
import Rx = require('rx');
import Observer = Rx.Observer;
import {ErrorReporter} from "./ui/errReporter";
import {PartialResult, ICancellable, RpcReply, formatDate} from "./util";
import {ProgressBar} from "./ui/progress";
import {FullPage} from "./ui/fullPage";
import {CombineOperators, RemoteObjectId} from "./javaBridge";
import {Test} from "./test";
import pako = require('pako');

/**
 * Path in server url for rpc web sockets.
 * This must match the web application server configuration.
 */
const RpcRequestPath = "rpc";

/**
 * Each remote object has a globally unique identifier.
 * (The initial remote object always has a fixed known identifier).
 * This is just a reference to a remote object.  All remote objects
 * are represented in Java by classes that extend RpcTarget.
 */
export class RemoteObject {
    constructor(public readonly remoteObjectId : RemoteObjectId) {}

    /**
     * Creates a request to a remote method.  The request must be invoked separately.
     * @param {string} method  Name of the method to invoke.  This method
     *                         in Java is tagged with the annotation @HillviewRPC
     *                         and has the signature:
     *                         public void method(RpcRequest request, RpcRequestContext context).
     * @param args             Arguments to pass to the method.  The arguments are converted
     *                         to JSON and show up on the Java side as a string encoding
     *                         in the 'arguments' field of the RpcRequest.  The Java side will
     *                         decode the string into a suitable datatype and call the corresponding
     *                         method in Java.
     * @returns {RpcRequest}   The request that is created.
     */
    createRpcRequest<T>(method: string, args: any) : RpcRequest<T> {
        return new RpcRequest<T>(this.remoteObjectId, method, args);
    }

    createStreamingRpcRequest<T>(method: string, args: any) : RpcRequest<PartialResult<T>> {
        return this.createRpcRequest<PartialResult<T>>(method, args);
    }

    /**
     * Combines the current RemoteObject with the currently
     * selected object (SelectedObject.current.getSelected)
     * according to the specified operation.  Should be overridden
     * in subclasses.
     * TODO: this seems out of place here.
     */
    combine(how: CombineOperators): void {}

    public toString(): string {
        return this.remoteObjectId;
    }
}

/**
 * A streaming RPC request: for each request made
 * we expect a stream of replies.  The requests are made
 * over web sockets.  When the last reply has been received
 * the web socket is closed.
 *
 * T is the type of the result returned.
 */
export class RpcRequest<T> implements ICancellable {
    readonly protoVersion : number = 6;
    readonly requestId: number;
    cancelled: boolean;
    closed:    boolean;  // i.e., not opened
    socket:    WebSocket;
    /**
     *  Time when RPC was initiated.  It may be set explicitly
     *  by users, and then it can be used to measured operations
     *  that span multiple RPCs */
    rpcTime:   Date;

    static requestCounter : number = 0;

    /**
     * Create a request to a remote object.
     * @param {string} objectId   Remote object unique identifier.
     * @param {string} method     Method that is being invoked.
     * @param args                Arguments that are passed to method; will be converted to JSON.
     */
    constructor(public objectId : string,
                public method : string,
                public args : any) {
        this.requestId = RpcRequest.requestCounter++;
        this.socket = null;
        this.cancelled = false;
        this.closed = true;
        this.rpcTime = null;
    }

    serialize() : Uint8Array {
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
        let str = JSON.stringify(result);
        console.log(formatDate() + " Sending message " + str);
        return pako.deflate(str);
    }

    /**
     * The time when this RPC request was invoked (or when the request it has been chained to
     * has started).
     */
    public startTime(): Date {
        return this.rpcTime;
    }

    setStartTime(start: Date): void{
        this.rpcTime = start;
    }

    /**
     * Indicates that this rpc request is executed as a continuation of
     * the specified operation.  This is mostly useful for accounting the time
     * required for executing a chain of operations.
     */
    public chain(after: ICancellable) {
        if (after != null)
            this.setStartTime(after.startTime());
    }

    /**
     * Cancel this RPC request.
     * @returns {boolean}  True if the request was not already completed.
     */
    public cancel(): boolean {
        if (!this.closed) {
            this.closed = true;
            this.socket.close();
            return true;
        }
        return false;
    }

    /**
     * Execute the RPC request and pass the results received to the specified observer.
     * @param onReply  An observer which is invoked for each result received by
     *                 the streaming RPC.
     */
    public invoke(onReply : Observer<T>) : void {
        try {
            // If time is not set we set it now.  The time may be set because this
            // is a request that is part of a chain of requests.
            if (this.rpcTime == null)
                this.rpcTime = new Date();
            // Create a web socked and send the request
            let rpcRequestUrl = "ws://" + window.location.hostname + ":" + window.location.port + "/" + RpcRequestPath;
            this.socket = new WebSocket(rpcRequestUrl);
            this.socket.binaryType = "arraybuffer";
            this.socket.onerror = function (ev: ErrorEvent) {
                console.log("socket error " + ev);
                let msg = ev.message;
                if (msg == null)
                    msg = "Error communicating to server.";
                onReply.onError(msg);
            };
            this.socket.onmessage = function (r: MessageEvent) {
                // parse json and invoke onReply.onNext
                console.log(formatDate() + ' reply received: ' + r.data);
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
                let reqStr: Uint8Array = this.serialize();
                this.socket.send(reqStr.buffer);
            };
            this.socket.onclose = (e: CloseEvent) => {
                console.log("Socket closed");
                if (e.code == 1000) {
                    onReply.onCompleted();
                    Test.instance.runNext();
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

/**
 * A Renderer is an abstract base class for observers that handle replies from
 * an Hillview streaming RPC request.  It receives a stream of PartialResult[T] objects.
 *
 * The protocol is that the onNext method will be called for each new piece
 * of data received.  If there is no error, the onCompleted method will be called last.
 * If an error occurs the onError method is called, and no other method may be called.
 */
export abstract class Renderer<T> implements Rx.Observer<PartialResult<T>> {
    protected progressBar: ProgressBar;
    protected reporter: ErrorReporter;

    /**
     * Create a Renderer.
     * @param {FullPage} page            This page will be used to show progress and to report errors.
     * @param {ICancellable} operation   The progress bar stop button can cancel this operation.
     *                                   (The operation may be null occasionally.)
     * @param {string} description       A description of the operation being performed that is shown
     *                                   next to the progress bar.
     */
    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string) {
        this.progressBar = page.progressManager.newProgressBar(operation, description);
        this.reporter = page.getErrorReporter();
        // TODO: This may be too eager.
        this.reporter.clear();
    }

    //noinspection JSUnusedLocalSymbols
    public makeSafe(disposable: Rx.IDisposable): Rx.Observer<PartialResult<T>> {
        return null;
    }

    /**
     * This method is called when all replies have been received
     * (successfully or unsuccesfully).
     */
    public finished(): void {
        // This causes the progress bar to disappear.
        this.progressBar.setFinished();
    }

    /**
     * Default implementation of the error handler.
     * @param exception  Exception message.
     */
    public onError(exception: any): void {
        // displays the error message using the reporter.
        this.reporter.reportError(String(exception));
        this.finished();
    }

    /**
     * This method is called when all replies have been received successfully.
     * Do not forget to call super.onCompleted() if you override this method;
     * otherwise the progress bar may never disappear.
     */
    public onCompleted(): void {
        this.page.scrollIntoView();
        this.finished();
    }

    /**
     * Method called whenever a new partial result is received.  Advances
     * the progress bar.  Do not forget to call super.onNext() if you override
     * this method; otherwise the progress bar will not advance.
     */
    public onNext(value: PartialResult<T>) {
        this.page.scrollIntoView();
        this.progressBar.setPosition(value.done);
        if (this.operation != null)
            console.log("onNext after " + this.elapsedMilliseconds());
    }

    /**
     * The number of milliseconds elapsed since the operation was initiated.
     * Note that the operation may have been 'chained' with another operation.
     */
    public elapsedMilliseconds(): number {
        return d3.timeMillisecond.count(this.operation.startTime(), new Date());
    }
}

/**
 * An OnCompleteRenderer does not do incremental renderings; it just
 * calls a 'run' method when all the data has been received.  It does
 * however manipulate correctly a progress bar.
 */
export abstract class OnCompleteRenderer<T> extends Renderer<T> {
    protected value: T = null;

    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string) {
        super(page, operation, description);
    }

    public onNext(value: PartialResult<T>) {
        super.onNext(value);
        if (value.data != null)
            this.value = value.data;
    }

    public onCompleted(): void {
        super.finished();
        this.page.scrollIntoView();
        if (this.value == null)
            return;
        this.run(this.value);
    }

    public abstract run(value: T): void;
}

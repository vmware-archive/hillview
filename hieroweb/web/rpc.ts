/// <reference path="node_modules/rx/ts/rx.d.ts" />
/// <reference path="typings/index.d.ts" />

import Rx = require('rx');
import RxDOM = require('rx-dom');
import Observer = Rx.Observer;
import Observable = Rx.Observable;
import {ErrorReporter, ConsoleErrorReporter} from "./errorReporter";

const HieroServiceUrl : string = "ws://localhost:8080";
const RpcRequestUrl = HieroServiceUrl + "/rpc";

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

// A streaming RPC request: for each request made
// we expect a stream of replies.  The requests are made
// over web sockets.  When the last reply has been received
// the web socked is closed.
export class RpcRequest {
    readonly protoVersion : number = 6;
    readonly requestId: number;
    socket   : any; // result of Rx.DOM.fromWebSocket.
    // Should be Rx.Subject<MessageEvent>, but this does not typecheck
    // the this.socket.onNext method with a String argument.

    static requestCounter : number = 0;

    constructor(public objectId : string,
                public method : string,
                public args : any) {
        this.requestId = RpcRequest.requestCounter++;
        this.socket = null;
    }

    serialize() : string {
        let result = {
            "objectId": this.objectId,
            "method": this.method,
            "arguments": JSON.stringify(this.args),
            "requestId": this.requestId,
            "protoVersion": this.protoVersion
        };
        return JSON.stringify(result);
    }

    private onOpen() : void {
        console.log('socket open');
        let reqStr = this.serialize();
        console.log("Sending message " + reqStr);
        this.socket.onNext(reqStr);
    }

    private static replyReceived<T>(replyEvent: MessageEvent, onReply: Observer<T>) : void {
        console.log('reply received: ' + replyEvent.data);
        let reply = <RpcReply>JSON.parse(replyEvent.data);
        if (reply.isError) {
            onReply.onError(reply.result);
        } else {
           let response = <T>JSON.parse(reply.result);
           onReply.onNext(response);
        }
    }

    // Function to call to execute the RPC.
    // onReply is an observer which is invoked for
    // each result received by the streaming RPC.
    public invoke<T>(onReply : Observer<T>) : void {
        // Invoked when the socked is opened
        let openObserver = Rx.Observer.create(() => this.onOpen());
        // Invoked when the socket is closed
        let closeObserver = Rx.Observer.create(function () {
            console.log('socket closing');
        });

        // Create a web socked and send the request
        this.socket = RxDOM.DOM.fromWebSocket(RpcRequestUrl, null, openObserver, closeObserver);
        console.log('socket created');
        this.socket.subscribe((r : MessageEvent) => RpcRequest.replyReceived(r, onReply));
    };
}

export abstract class RpcReceiver<T> implements Rx.Observer<T> {
    public constructor(public reporter: ErrorReporter) {
        if (this.reporter == null)
            this.reporter = ConsoleErrorReporter.instance;
    }

    public makeSafe(disposable: Rx.IDisposable): Rx.Observer<T> {
        return null;
    }

    public abstract onNext(value: T): void;

    public onError(exception: any): void {
        this.reporter.reportError(String(exception));
    }

    public onCompleted(): void {}
}

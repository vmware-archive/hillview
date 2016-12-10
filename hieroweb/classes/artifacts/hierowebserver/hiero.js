define(["require", "exports", "rx.all.js", "rx.dom.js"], function (require, exports, rx_all_js_1, rx_dom_js_1) {
    "use strict";
    var HieroServiceUrl = "ws://localhost:8080";
    var RpcRequestUrl = HieroServiceUrl + "/rpc";
    // A streaming RPC request: for each request made
    // we expect a stream of replies.  The requests are made
    // over web sockets.  When the last reply has been received
    // the web socked is closed.
    var RpcRequest = (function () {
        function RpcRequest(objectId, method, args) {
            this.objectId = objectId;
            this.method = method;
            this.args = args;
            this.version = 5;
            this.requestId = RpcRequest.requestCounter++;
            this.socket = null;
        }
        RpcRequest.prototype.onOpen = function () {
            console.log('socket open');
            var reqStr = JSON.stringify(this);
            console.log("Sending message " + reqStr);
            this.socket.onNext(reqStr);
        };
        // Function to call to execute the RPC.
        // onReply is the continuation function which is invoked for
        // each result received by the streaming RPC.
        RpcRequest.prototype.invoke = function (onReply) {
            // Invoked when the socked is opened
            var openObserver = rx_all_js_1.default.Observer.create(this.onOpen);
            // Invoked when the socket is closed
            var closeObserver = rx_all_js_1.default.Observer.create(function (unused) {
                console.log('socket closing');
            });
            // Create a web socked and send the request
            this.socket = rx_dom_js_1.default.fromWebSocket(RpcRequestUrl, null, openObserver, closeObserver);
            console.log('socket created');
            this.socket.subscribe(onReply, function (e) { console.log('error: ' + e.toString()); }, function () { console.log('socket closed'); });
        };
        ;
        return RpcRequest;
    }());
    RpcRequest.requestCounter = 0;
    exports.RpcRequest = RpcRequest;
});

function RpcRequest(objectId, method, arguments) {
    this.requestId = RpcRequest.requestCounter++;
    this.objectId = objectId;
    this.method = method;
    this.arguments = arguments;
}

RpcRequest.requestCounter = 0;

<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <script src="rx.all.js"></script>
    <script src="rx.dom.js"></script>
    <script src="hiero.js"></script>
    <script>
        function onData(data) {
            console.log("onData: " + data.data);
            var span = document.getElementById("cell");
            span.textContent += "{" + data.data + "}";
            if (data.data === "0")
                socket.onCompleted();
        };

        function cellClick() {
            console.log("cellClick");
            var span = document.getElementById("cell");
            span.textContent += "subscribed";
            var openObserver = Rx.Observer.create(function (e) {
                console.log('socket open');
                var req = new RpcRequest(0, "none", null);
                var reqStr = JSON.stringify(req);
                console.log("Sending message " + reqStr);
                socket.onNext(reqStr);
            });
            var closeObserver = Rx.Observer.create(function (e) {
                console.log('socket closing');
            });
            var address = "ws://localhost:8080/rpc";
            socket = Rx.DOM.fromWebSocket(address, null, openObserver, closeObserver);
            console.log('socket created');

            socket.subscribe(
                    onData,
                    function(e) { console.log('error: ' + e.toString()); },
                    function() { console.log('socket closed'); }
            );
        };
    </script>
    <link rel="stylesheet" href="hiero.css">
</head>
<body>
    <span id="cell" onclick="cellClick()">[ ]</span>
</body>
</html>

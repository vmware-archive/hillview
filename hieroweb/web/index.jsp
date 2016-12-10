<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <!-- Library for loading other JavaScript programs -->
    <script src="require.js"></script>
    <script>
        requirejs(["hiero.js"]);

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
            var req = new RpcRequest(0, "none", null);
            req.invoke(onData);
        };
    </script>
    <link rel="stylesheet" href="hiero.css">
</head>
<body>
    <span id="cell" onclick="cellClick()">[ ]</span>
</body>
</html>

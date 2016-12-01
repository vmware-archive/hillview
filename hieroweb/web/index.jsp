<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <script src="http://cdnjs.cloudflare.com/ajax/libs/rxjs/4.1.0/rx.all.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/rxjs-dom/7.0.3/rx.dom.js"></script>
    <script>
        function onData(data) {
            console.log("onData");
            var span = document.getElementById("cell");
            span.textContent += "{" + data + "}";
        };
        function cellClick() {
            var span = document.getElementById("cell");
            span.textContent += "subscribed";
            console.log("cellClick");
            Rx.DOM.getJSON("/json").subscribe(onData);
        };
    </script>
    <link rel="stylesheet" href="hiero.css">
</head>
<body>
    <span id="cell" onclick="cellClick()">[]</span>
    <!--
    <script src="hiero.js"></script>
    -->
</body>
</html>

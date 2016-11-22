<%@ page import="org.hiero.web.Hello" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <script>function insert() {
        var span = document.getElementById("insert");
        span.textContent = "inserted";
    }
    </script>
    <link rel="stylesheet" href="hiero.css">
</head>
<body>
    <h3><%=Hello.getMessage()%></h3>
    <P>There</P>
    <span id="insert" onclick="insert()">[]</span>
    <div id="textContainer"></div>
    <script src="http://cdnjs.cloudflare.com/ajax/libs/rxjs/4.1.0/rx.all.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/rxjs-dom/7.0.3/rx.dom.js"></script>
    <script src="hiero.js"></script>
</body>
</html>

package org.hiero.webapi;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class WebServer {
    private final static Logger LOGGER = Logger.getLogger(WebServer.class.getName());

    public static HttpServer createServer(int port) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", new MyHandler());
        server.setExecutor(null); // creates a default executor
        return server;
    }

    static class MyHandler implements HttpHandler {
        String dispatchPath(String path) {
            String response = "Server is up";
            return response;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            URI uri = exchange.getRequestURI();
            LOGGER.info("Received request " + uri.toString());

            String reply = dispatchPath(uri.getPath());
            if (reply != null) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, reply.length());
            } else {
                reply = "Error";
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, reply.length());
            }
            OutputStream os = exchange.getResponseBody();
            os.write(reply.getBytes());
            os.close();
        }
    }
}

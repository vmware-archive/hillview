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

    public static HttpServer createServer(final int port) throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", new RootHandler());
        server.createContext("/inc", new IncHandler());
        server.setExecutor(null); // creates a default executor
        return server;
    }

    static class RootHandler implements HttpHandler {
        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final String reply = "Welcome to hiero";
            final OutputStream os = exchange.getResponseBody();
            os.write(reply.getBytes());
            os.close();
        }
    }

    static void error(final HttpExchange exchange, final String message) {
        try {
            LOGGER.severe(message);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, message.length());
            final OutputStream os = exchange.getResponseBody();
            os.write(message.getBytes());
            os.close();
        } catch (final IOException ioex) {
            LOGGER.severe("IO exception " + ioex.toString());
        }
    }

    static class IncHandler implements HttpHandler {
        String execute(final HttpExchange exchange, final String args) {
            try {
                final int i = Integer.parseInt(args);
                return Integer.toString(i+1);
            } catch (final Exception ex) {
                error(exchange, ex.toString());
                return null;
            }
        }

        @Override
        public void handle(final HttpExchange exchange) throws IOException {
            final URI uri = exchange.getRequestURI();
            LOGGER.info("Received request " + uri.toString());

            final String reply = execute(exchange, uri.getPath());
            if (reply != null) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, reply.length());
                final OutputStream os = exchange.getResponseBody();
                os.write(reply.getBytes());
                os.close();
            }
        }
    }
}

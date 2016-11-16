package org.hiero.webapi;

import com.sun.net.httpserver.HttpServer;

/**
 * Main entry point for the Hiero service.
 */
public class Main {
    public static void main(final String[] args) {
        try {
            runserver();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void runserver() throws Exception {
        final HttpServer server = WebServer.createServer(8000);
        server.start();
    }
}

package org.hiero.webapi;

import com.sun.net.httpserver.HttpServer;

/**
 * Main entry point for the Hiero service.
 */
public class Main {
    public static void main(String[] args) {
        try {
            runserver();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void runserver() throws Exception {
        HttpServer server = WebServer.createServer(8000);
        server.start();
    }
}

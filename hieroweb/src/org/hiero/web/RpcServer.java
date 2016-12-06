package org.hiero.web;

import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A server which implements RPC calls between a web browser client and
 * a Java-based web server.
 */
@ServerEndpoint(value = "/rpc")
public class RpcServer {
    static private final int version = 2;
    private final HashMap<String, RpcTarget> objects;

    public RpcServer() {
        this.objects = new HashMap<String, RpcTarget>();
    }

    private void addObject(@NonNull RpcTarget object) {
        if (this.objects.containsKey(object.objectId))
            throw new RuntimeException("Object with id " + object.objectId + " already in map");
        this.objects.put(object.objectId, object);
    }

    private RpcTarget getObject(String id) {
        return this.objects.get(id);
    }

    private void deleteObject(String id) {
        if (!this.objects.containsKey(id))
            throw new RuntimeException("Object with id " + id + " does not exist");
        this.objects.remove(id);
    }

    private static final Logger LOGGER =
            Logger.getLogger(RpcServer.class.getName());

    @OnOpen
    public void onOpen(@NonNull Session session) {
        LOGGER.log(Level.INFO, "Server " + Integer.toString(version) +
                        " new connection with client: {0}",
                session.getId());
    }

    @OnMessage
    public void onMessage(@NonNull String message, @NonNull Session session) {
        LOGGER.log(Level.FINE, "New message from Client [{0}]: {1}",
                new Object[] {session.getId(), message});

        RpcRequest req;
        try {
            Reader reader = new StringReader(message);
            JsonReader jreader = new JsonReader(reader);
            JsonElement elem = Streams.parse(jreader);
            req = new RpcRequest(elem);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error processing json: ", ex);
            replyWithError(ex, session);
            return;
        }

        execute(req, session);
    }

    public void sendReply(@NonNull RpcReply reply, @NonNull Session session) {
        try {
            JsonElement json = reply.toJson();
            session.getBasicRemote().sendText(json.toString());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not send reply");
        }
    }

    private void closeSession(@NonNull Session session) {
        try {
            session.close();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error closing session");
        }
    }

    private void execute(@NonNull RpcRequest rpcRequest, @NonNull Session session) {
        LOGGER.log(Level.INFO, "Executing " + rpcRequest.toString());
        try {
            RpcTarget stub = this.getObject(rpcRequest.objectId);
            if (stub == null)
                throw new RuntimeException("No object with id " + rpcRequest.objectId);
            stub.execute(rpcRequest, session);
        } catch (Exception ex) {
            replyWithError(ex, session);
        }
        closeSession(session);
    }

    private void replyWithError(final Throwable th, final Session session) {
        final RpcReply reply = new RpcReply(-1, "Error: " + th.toString());
        sendReply(reply, session);
        closeSession(session);
    }

    @OnClose
    public void onClose(final Session session) {
        LOGGER.log(Level.FINE, "Close connection for client: {0}", session.getId());
    }

    @OnError
    public void onError(final Throwable exception, final Session unused) {
        LOGGER.log(Level.SEVERE, "Error: ", exception);
    }
}
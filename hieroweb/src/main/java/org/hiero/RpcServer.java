package hiero.web;

import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.*;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A server which implements RPC calls between a web browser client and
 * a Java-based web server.
 */
@ServerEndpoint(value = "/rpc")
public final class RpcServer {
    static private final int version = 2;
    // Used to generate fresh object ids
    private int objectIdGenerator;
    // Map object id to object.
    private final HashMap<String, RpcTarget> objects;
    private static final Logger LOGGER =
            Logger.getLogger(RpcServer.class.getName());

    public RpcServer() {
        this.objects = new HashMap<String, RpcTarget>();
        this.objectIdGenerator = 1;
        // The initial object must start with a well-known object id
        InitialObject initial = new InitialObject("0");
        this.addObject(initial);
    }

    public String freshId() {
        while (true) {
            String id = Integer.toString(this.objectIdGenerator);
            if (!this.objects.containsKey(id))
                return id;
            this.objectIdGenerator++;
        }
    }

    public void addObject(@NonNull RpcTarget object) {
        if (this.objects.containsKey(object.objectId))
            throw new RuntimeException("Object with id " + object.objectId + " already in map");
        this.objects.put(object.objectId, object);
        object.setServer(this);
    }

    private @NonNull RpcTarget getObject(String id) {
        RpcTarget target = this.objects.get(id);
        if (target == null)
            throw new RuntimeException("RPC target " + id + " is unknown");
        return target;
    }

    @SuppressWarnings("unused")
    private void deleteObject(String id) {
        if (!this.objects.containsKey(id))
            throw new RuntimeException("Object with id " + id + " does not exist");
        this.objects.remove(id);
    }

    @SuppressWarnings("unused")
    @OnOpen
    public void onOpen(@NonNull Session session) {
        LOGGER.log(Level.INFO, "Server " + Integer.toString(version) +
                        " new connection with client: {0}",
                session.getId());
    }

    @SuppressWarnings("unused")
    @OnMessage
    public void onMessage(@NonNull String message, @NonNull Session session) {
        LOGGER.log(Level.INFO, "New message from Client [{0}]: {1}",
                new Object[] {session.getId(), message});

        RpcRequest req;
        try {
            Reader reader = new StringReader(message);
            JsonReader jreader = new JsonReader(reader);
            JsonElement elem = Streams.parse(jreader);
            req = new RpcRequest(elem);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error processing json: ", ex);
            this.replyWithError(ex, session);
            return;
        }

        this.execute(req, session);
    }

    void sendReply(@NonNull RpcReply reply, @NonNull Session session) {
        try {
            JsonElement json = reply.toJson();
            session.getBasicRemote().sendText(json.toString());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Could not send reply");
        }
    }

    static String asString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    private void execute(@NonNull RpcRequest rpcRequest, @NonNull Session session) {
        LOGGER.log(Level.INFO, "Executing " + rpcRequest.toString());

        try {
            RpcTarget stub = this.getObject(rpcRequest.objectId);
            if (stub == null)
                throw new RuntimeException("RpcServer.getObject() returned null");
            // This sends the reply and closes the session.
            stub.execute(rpcRequest, session);
        } catch (Exception ex) {
            RpcReply reply = rpcRequest.createReply(ex);
            this.sendReply(reply, session);
            rpcRequest.closeSession(session);
        }
    }

    private void closeSession(@NonNull Session session) {
        try {
            session.close();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error closing session");
        }
    }

    private void replyWithError(final Throwable th, final Session session) {
        final RpcReply reply = new RpcReply(-1, asString(th), true);
        this.sendReply(reply, session);
        this.closeSession(session);
    }

    @SuppressWarnings("unused")
    @OnClose
    public void onClose(final Session session) {
        LOGGER.log(Level.FINE, "Close connection for client: {0}", session.getId());
    }

    @SuppressWarnings("unused")
    @OnError
    public void onError(final Throwable exception, final Session unused) {
        LOGGER.log(Level.SEVERE, "Error: ", exception);
    }
}
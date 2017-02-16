package hiero.web;

import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A server which implements RPC calls between a web browser client and
 * a Java-based web server.  The web server could create a different
 * instance of this class for each request.
 */
@ServerEndpoint(value = "/rpc")
public final class RpcServer {
    static private final int version = 2;
    private static final Logger logger =
            Logger.getLogger(RpcServer.class.getName());

    @SuppressWarnings("unused")
    @OnOpen
    public void onOpen(@NonNull Session session) {
        logger.log(Level.INFO, "Server " + Integer.toString(version) +
                        " new connection with client: {0}",
                session.getId());
    }

    @SuppressWarnings("unused")
    @OnMessage
    public void onMessage(@NonNull String message, @NonNull Session session) {
        logger.log(Level.INFO, "New message from Client [{0}]: {1}",
                new Object[] {session.getId(), message});

        RpcRequest req;
        try {
            Reader reader = new StringReader(message);
            JsonReader jreader = new JsonReader(reader);
            JsonElement elem = Streams.parse(jreader);
            req = new RpcRequest(elem);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error processing json: ", ex);
            this.replyWithError(ex, session);
            return;
        }

        this.execute(req, session);
    }

    private void sendReply(@NonNull RpcReply reply, @NonNull Session session) {
        try {
            JsonElement json = reply.toJson();
            session.getBasicRemote().sendText(json.toString());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not send reply");
        }
    }

    static String asString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    private void execute(@NonNull RpcRequest rpcRequest, @NonNull Session session) {
        logger.log(Level.INFO, "Executing " + rpcRequest.toString());
        try {
            RpcTarget target = RpcObjectManager.instance.getObject(rpcRequest.objectId);
            if (target == null)
                throw new RuntimeException("RpcServer.getObject() returned null");
            // This function is responsible for sending the replies and closing the session.
            target.execute(rpcRequest, session);
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
            logger.log(Level.SEVERE, "Error closing session");
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
        logger.log(Level.FINE, "Close connection for client: {0}", session.getId());
    }

    @SuppressWarnings("unused")
    @OnError
    public void onError(final Throwable exception, final Session unused) {
        logger.log(Level.SEVERE, "Error: ", exception);
    }
}
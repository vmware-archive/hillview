package hiero.web;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.Session;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RpcReply {
    private final int requestId;
    private final String result;
    private final boolean isError;
    private static final Logger logger =
            Logger.getLogger(RpcReply.class.getName());

    public RpcReply(final int requestId, final String result, boolean isError) {
        this.requestId = requestId;
        this.result = result;
        this.isError = isError;
    }

    public JsonElement toJson() {
        JsonObject result = new JsonObject();
        result.addProperty("requestId", this.requestId);
        result.addProperty("result", this.result);
        result.addProperty("isError", this.isError);
        return result;
    }

    public void send(@NonNull Session session) {
        try {
            JsonElement json = this.toJson();
            session.getBasicRemote().sendText(json.toString());
            RpcReply.logger.log(Level.INFO, "Reply sent");
        } catch (IOException e) {
            RpcReply.logger.log(Level.SEVERE, "Could not send reply");
        }
    }
}


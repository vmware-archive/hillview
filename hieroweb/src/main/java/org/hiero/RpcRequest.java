package org.hiero;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IJson;

import javax.websocket.Session;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RpcRequest {
    private static final Logger LOGGER =
            Logger.getLogger(RpcRequest.class.getName());

    public final int    requestId;
    @NonNull
    public final String objectId;
    @NonNull public final String method;
    @NonNull public final String arguments;  // A JSON string

    public RpcRequest(@NonNull JsonElement element) {
        final JsonObject obj = element.getAsJsonObject();
        this.requestId = obj.get("requestId").getAsInt();
        this.objectId = obj.get("objectId").getAsString();
        this.method = obj.get("method").getAsString();
        this.arguments = obj.get("arguments").getAsString();
    }

    @Override
    public String toString() {
        return "RpcRequest: " + this.objectId + "." + this.method + "()";
    }

    public RpcReply createReply(String json) {
        return new RpcReply(this.requestId, json, false);
    }

    public RpcReply createReply(JsonElement userResult) {
        return this.createReply(userResult.toString());
    }

    public RpcReply createReply(IJson userResult) {
        return this.createReply(userResult.toJson());
    }

    public RpcReply createReply(Throwable th) {
        return new RpcReply(this.requestId, this.toString() + "\n" + RpcServer.asString(th), true);
    }

    public void closeSession(@NonNull Session session) {
        try {
            session.close();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error closing session");
        }
    }
}
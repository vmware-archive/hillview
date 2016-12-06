package org.hiero.web;

import com.google.gson.JsonElement;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.Session;
import java.io.IOException;
import java.util.logging.Level;

public abstract class RpcTarget {
    public final String objectId;
    public final RpcServer server;

    RpcTarget(@NonNull String objectId, @NonNull RpcServer server) {
        this.objectId = objectId;
        this.server = server;
    }

    abstract void execute(@NonNull RpcRequest request, @NonNull Session session);

    @Override
    public int hashCode() { return this.objectId.hashCode(); }
}

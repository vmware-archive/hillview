package org.hiero.web;

import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.Session;

public class SampleTarget extends RpcTarget {
    public SampleTarget(@NonNull String objectId,
                        @NonNull RpcServer server) {
        super(objectId, server);
    }

    public void execute(@NonNull RpcRequest request, @NonNull Session session) {
        // TODO: dispatch here to the right procedure
        int replies = 3;
        for (int i = 1; i < replies; i++) {
            RpcReply reply = request.createReply(
                    request.requestId + " " + Integer.toString(i));
            this.server.sendReply(reply, session);
        }
    }
}

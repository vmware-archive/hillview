package org.hiero.web;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class RpcReply {
    private final int requestId;
    private final String result;

    public RpcReply(final int requestId, final String result) {
        this.requestId = requestId;
        this.result = result;
    }

    public JsonElement toJson() {
        JsonObject result = new JsonObject();
        result.addProperty("requestId", this.requestId);
        result.addProperty("result", this.result);
        return result;
    }
}


package org.hiero;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class RpcReply {
    private final int requestId;
    private final String result;
    private final boolean isError;

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
}

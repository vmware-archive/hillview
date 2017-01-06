import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.NonNull;

public class RpcRequest {
    public final int    requestId;
    @NonNull
    public final String objectId;
    @NonNull private final String method;
    private final String[] arguments;

    public RpcRequest(@NonNull JsonElement element) {
        final JsonObject obj = element.getAsJsonObject();
        this.requestId = obj.get("requestId").getAsInt();
        this.objectId = obj.get("objectId").getAsString();
        this.method = obj.get("method").getAsString();
        JsonArray array = obj.get("arguments").getAsJsonArray();
        if (array == null) {
            this.arguments = null;
        } else {
            this.arguments = new String[array.size()];
            for (int i=0; i < array.size(); i++)
                this.arguments[i] = array.get(i).getAsString();
        }
    }

    @Override
    public String toString() {
        return "RpcRequest: " + this.objectId + "." + this.method + "()";
    }

    public RpcReply createReply(String userResult) {
        return new RpcReply(this.requestId, userResult);
    }
}
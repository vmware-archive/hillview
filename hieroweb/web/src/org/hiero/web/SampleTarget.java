import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.Session;

public class SampleTarget extends RpcTarget {
    public SampleTarget(@NonNull String objectId) {
        super(objectId);
    }

    public void execute(@NonNull RpcRequest request, @NonNull Session session) {
        // TODO: dispatch here to the right procedure
        int replies = 3;
        for (int i = 1; i < replies; i++) {
            RpcReply reply = request.createReply(Integer.toString(i));
            this.server.sendReply(reply, session);
        }
    }
}

package hiero.web;

import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.dataset.api.PartialResult;
import rx.Observer;

import javax.websocket.Session;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class RpcTarget {
    protected String objectId;
    private HashMap<String, Method> executor;
    protected static Logger logger = Logger.getLogger(RpcTarget.class.getName());

    RpcTarget() {
        this.executor = new HashMap<String, Method>();
        this.registerExecutors();
    }

    public void setId(String objectId) {
        this.objectId = objectId;
    }

    private void registerExecutors() {
        // use reflection to register all methods that have an @HieroRpc annotation
        // as executors
        Class<?> type = this.getClass();
        for (Method m : type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(HieroRpc.class)) {
                logger.log(Level.INFO, "Registered RPC method " + m.getName());
                this.executor.put(m.getName(), m);
            }
        }
    }

    // Dispatches an RPC request for execution
    public void execute(@NonNull RpcRequest request, @NonNull Session session)
            throws InvocationTargetException, IllegalAccessException {
        Method cons = this.executor.get(request.method);
        if (cons == null)
            throw new RuntimeException("No such method " + request.method);
        cons.invoke(this, request, session);
    }

    @Override
    public int hashCode() { return this.objectId.hashCode(); }

    class ResultObserver<T extends IJson> implements Observer<PartialResult<T>> {
        @NonNull final RpcRequest request;
        @NonNull final Session session;

        // TODO: handle session disconnections

        ResultObserver(@NonNull RpcRequest request, @NonNull Session session) {
            this.request = request;
            this.session = session;
        }

        @Override
        public void onCompleted() {
            this.request.closeSession(this.session);
        }

        @Override
        public void onError(Throwable throwable) {
            if (!this.session.isOpen()) return;

            RpcTarget.logger.log(Level.SEVERE, throwable.toString());
            RpcReply reply = this.request.createReply(throwable);
            reply.send(this.session);
        }

        @Override
        public void onNext(PartialResult<T> pr) {
            if (!this.session.isOpen()) return;

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            json.add("data", pr.deltaValue.toJsonTree());
            RpcReply reply = this.request.createReply(json);
            reply.send(this.session);
        }
    }

    @Override
    public String toString() {
        return "id: " + this.objectId;
    }

    public String idToJson() {
        return RpcObjectManager.gson.toJson(this.objectId);
    }
}

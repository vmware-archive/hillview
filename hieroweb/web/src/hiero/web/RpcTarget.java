package hiero.web;

import org.checkerframework.checker.nullness.qual.NonNull;

import javax.websocket.Session;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class RpcTarget {
    protected final String objectId;
    protected RpcServer server;
    private HashMap<String, Method> executor;
    protected Logger logger = Logger.getLogger(RpcTarget.class.getName());

    RpcTarget(@NonNull String objectId) {
        this.objectId = objectId;
        this.executor = new HashMap<String, Method>();
        this.registerExecutors();
    }

    public void setServer(@NonNull RpcServer server) {
        this.server = server;
    }

    private void registerExecutors() {
        // use reflection to register all methods that have an @rpcexecute annotation
        // as executors
        Class<?> type = this.getClass();
        for (Method m : type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(HieroRpc.class)) {
                this.logger.log(Level.INFO, "Registered RPC method " + m.getName());
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
}

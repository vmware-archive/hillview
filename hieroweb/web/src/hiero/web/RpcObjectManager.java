package hiero.web;

import com.google.gson.Gson;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

// Singleton pattern.
// Manages all RpcTarget objects.
public class RpcObjectManager {
    public static final RpcObjectManager instance;
    public static final Gson gson;
    private static final Logger LOGGER;

    static {
        LOGGER = Logger.getLogger(RpcObjectManager.class.getName());
        instance = new RpcObjectManager();
        gson = new Gson();
    }

    // Used to generate fresh object ids
    private int objectIdGenerator;
    // Map object id to object.
    private final HashMap<String, RpcTarget> objects;

    private RpcObjectManager() {
        this.objects = new HashMap<String, RpcTarget>();
        this.objectIdGenerator = 0;

        InitialObject initial = new InitialObject();
        this.addObject(initial);
    }

    private String freshId() {
        while (true) {
            String id = Integer.toString(this.objectIdGenerator);
            if (!this.objects.containsKey(id))
                return id;
            this.objectIdGenerator++;
        }
    }

    synchronized public void addObject(@NonNull RpcTarget object) {
        String id = this.freshId();
        object.setId(id);
        if (this.objects.containsKey(object.objectId))
            throw new RuntimeException("Object with id " + object.objectId + " already in map");
        LOGGER.log(Level.INFO, "Inserting target " + object.toString());
        this.objects.put(object.objectId, object);
    }

    synchronized public @NonNull RpcTarget getObject(@NonNull String id) {
        LOGGER.log(Level.INFO, "Getting object " + id);
        if (id == null)
            throw new RuntimeException("Null object id");
        RpcTarget target = this.objects.get(id);
        if (target == null)
            throw new RuntimeException("RPC target " + id + " is unknown");
        return target;
    }

    @SuppressWarnings("unused")
    synchronized public void deleteObject(@NonNull String id) {
        if (!this.objects.containsKey(id))
            throw new RuntimeException("Object with id " + id + " does not exist");
        this.objects.remove(id);
    }
}

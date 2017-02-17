package org.hiero;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The RpcObjectManager manages a pool of objects that are the targets of RPC calls
 * from the clients.  These are RpcTarget objects, and each one has a unique
 * identifier.  This class manages these identifiers and keeps track of the mapping
 * between identifiers and objects.
 * This is a singleton pattern.
 */
public final class RpcObjectManager {
    // We have exactly one instance of this object, because the web server
    // is multithreaded and it instantiates various classes on demand to service requests.
    // These need to be able to find the ObjectManager - they do it through
    // the unique global instance.
    public static final RpcObjectManager instance;
    protected static final Gson gson;
    private static final Logger LOGGER;

    static {
        LOGGER = Logger.getLogger(RpcObjectManager.class.getName());
        instance = new RpcObjectManager();
        gson = new Gson();
        new InitialObject();  // indirectly registers this object with the RpcObjectManager
    }

    // Used to generate fresh object ids
    private int objectIdGenerator;
    // Map object id to object.
    private final HashMap<String, RpcTarget> objects;

    // Private constructor
    private RpcObjectManager() {
        this.objects = new HashMap<String, RpcTarget>();
        this.objectIdGenerator = 0;
    }

    /**
     * Allocate a fresh identifier.
     */
    private String freshId() {
        while (true) {
            String id = Integer.toString(this.objectIdGenerator);
            if (!this.objects.containsKey(id))
                return id;
            this.objectIdGenerator++;
        }
    }

    synchronized public void addObject(RpcTarget object) {
        String id = this.freshId();
        object.setId(id);
        if (this.objects.containsKey(object.objectId))
            throw new RuntimeException("Object with id " + object.objectId + " already in map");
        LOGGER.log(Level.INFO, "Inserting target " + object.toString());
        this.objects.put(object.objectId, object);
    }

    synchronized public RpcTarget getObject(String id) {
        LOGGER.log(Level.INFO, "Getting object " + id);
        if (id == null)
            throw new RuntimeException("Null object id");
        RpcTarget target = this.objects.get(id);
        if (target == null)
            throw new RuntimeException("RPC target " + id + " is unknown");
        return target;
    }

    @SuppressWarnings("unused")
    synchronized public void deleteObject(String id) {
        if (!this.objects.containsKey(id))
            throw new RuntimeException("Object with id " + id + " does not exist");
        this.objects.remove(id);
    }
}

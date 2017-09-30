/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import org.hillview.utils.HillviewLogging;
import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.*;

/**
 * The RpcObjectManager manages a pool of objects that are the targets of RPC calls
 * from the clients.  These are RpcTarget objects, and each one has a unique
 * identifier.  This class manages these identifiers and keeps track of the mapping
 * between identifiers and objects.
 *
 * The class also keeps track of open sessions and matches sessions to RpcTargets.
 *
 * This is a singleton pattern.
 */
public final class RpcObjectManager {
    /**
     * Well-known id of the initial object.
     */
    static final String initialObjectId = "0";

    // We have exactly one instance of this object, because the web server
    // is multi-threaded and it instantiates various classes on demand to service requests.
    // These need to be able to find the ObjectManager - they do it through
    // the unique global instance.
    public static final RpcObjectManager instance;

    // Map object id to object.
    private final HashMap<String, RpcTarget> objects;

    // Map the session to the targetId object that is replying, if any
    private final HashMap<Session, RpcTarget> sessionRequest =
            new HashMap<Session, RpcTarget>(10);
    // Mapping sessions to RxJava subscriptions - needed to do cancellations.
    private final HashMap<Session, Subscription> sessionSubscription =
            new HashMap<Session, Subscription>(10);

    // TODO: persist object history into persistent storage.
    // For each object id the computation that has produced it.
    private final HashMap<String, HillviewComputation> generator =
            new HashMap<String, HillviewComputation>();
    // For each computation keep its result.
    private final HashMap<HillviewComputation, String> memoized =
            new HashMap<HillviewComputation, String>();

    synchronized void addSession(Session session, @Nullable RpcTarget target) {
        this.sessionRequest.put(session, target);
    }

    synchronized String checkCache(HillviewComputation computation) {
        return this.memoized.get(computation);
    }

    synchronized void removeSession(Session session) {
        this.sessionRequest.remove(session);
    }

    @Nullable synchronized RpcTarget getTarget(Session session) {
        return this.sessionRequest.get(session);
    }

    @Nullable synchronized Subscription getSubscription(Session session) {
        return this.sessionSubscription.get(session);
    }

    synchronized void addSubscription(Session session, Subscription subscription) {
        if (subscription.isUnsubscribed())
            // The computation may have already finished by the time we get here!
            return;
        HillviewLogging.logger().info("Saving subscription {}", this.toString());
        if (this.sessionSubscription.get(session) != null)
            throw new RuntimeException("Subscription already active on this session");
        this.sessionSubscription.put(session, subscription);
    }

    synchronized void removeSubscription(Session session) {
        HillviewLogging.logger().info("Removing subscription {}", this.toString());
        this.sessionSubscription.remove(session);
    }

    static {
        instance = new RpcObjectManager();
        new InitialObjectTarget();  // indirectly registers this object with the RpcObjectManager
    }

    // Private constructor
    private RpcObjectManager() {
        this.objects = new HashMap<String, RpcTarget>();
    }

    /**
     * Allocate a fresh identifier.
     */
    private String freshId() {
        return UUID.randomUUID().toString();
    }

    synchronized void addObject(RpcTarget object) {
        if (object.objectId == null) {
            String id = this.freshId();
            object.setId(id);
        }
        if (this.objects.containsKey(object.objectId))
            // TODO: attempt to reconstruct missing object
            throw new RuntimeException("Object with id " + object.objectId + " already in map");
        this.generator.put(object.objectId, object.computation);
        this.memoized.put(object.computation, object.objectId);
        HillviewLogging.logger().info("Inserting targetId {}", object.toString());
        this.objects.put(object.objectId, object);
        this.reconstructing.remove(object.objectId);
    }

    private synchronized @Nullable RpcTarget getObject(String id) {
        HillviewLogging.logger().info("Getting object {}", id);
        return this.objects.get(id);
    }

    /**
     * Set of targets that are under reconstruction.
     * Kept here to prevent infinite looks.
     */
    private Set<String> reconstructing = new HashSet<String>();

    /**
     * @return False if the element is already under reconstruction.
     */
    synchronized private boolean addReconstructing(String id) {
        return this.reconstructing.add(id);
    }

    synchronized private void removeReconstructing(String id) {
        this.reconstructing.remove(id);
    }

    /**
     * Attempt to retrieve the object with the specified id.
     * @param id           Object id to retrieve.
     * @param reconstruct  If true and the object cannot be retrieved attempt
     *                     to reconstruct the object based on the history.
     * @return             An object, or throws if the object cannot be obtained.
     */
    RpcTarget retrieveTarget(String id, boolean reconstruct) {
        RpcTarget target = this.getObject(id);
        if (target != null)
            return target;
        if (!reconstruct)
            throw new RuntimeException("Cannot find object " + id);
        this.rebuild(id);
        target = this.getObject(id);
        if (target == null)
            throw new RuntimeException("Cannot reconstruct object " + id);
        return target;
    }

    private void rebuild(String id) {
        HillviewLogging.logger().info("Attempt to reconstruct {}", id);
        boolean working = this.addReconstructing(id);
        if (working) {
            // TODO: wait for reconstruction to finish somehow
            // bailing out here means that we fail.
            HillviewLogging.logger().warn("Already reconstructing {}; giving up", id);
            return;
        }

        Set<String> recursive = new HashSet<String>();
        HillviewComputation computation = this.generator.get(id);
        if (computation != null)
            // The following may trigger a recursive reconstruction.
            computation.replay();
        // The invariant is that only the one who added the object
        // to the reconstruction list will remove it.
        this.removeReconstructing(id);
    }

    @SuppressWarnings("unused")
    synchronized private void deleteObject(String id) {
        if (!this.objects.containsKey(id))
            throw new RuntimeException("Object with id " + id + " does not exist");
        this.objects.remove(id);
    }

    /**
     * Removes all RemoteObjects from the cache, except the initial object.
     * @return  The number of objects removed.
     */
    int removeAllObjects() {
        List<String> toDelete = new ArrayList<String>();
        for (String k: this.objects.keySet()) {
            if (!k.equals(initialObjectId))
                toDelete.add(k);
        }

        for (String k: toDelete)
            this.deleteObject(k);
        return toDelete.size();
    }
}

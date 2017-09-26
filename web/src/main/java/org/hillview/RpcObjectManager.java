/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview;

import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    // We have exactly one instance of this object, because the web server
    // is multi-threaded and it instantiates various classes on demand to service requests.
    // These need to be able to find the ObjectManager - they do it through
    // the unique global instance.
    public static final RpcObjectManager instance;
    private static final Logger LOGGER;

    // Map the session to the target object that is replying, if any
    private final HashMap<Session, RpcTarget> sessionRequest =
            new HashMap<Session, RpcTarget>(10);
    // Mapping sessions to RxJava subscriptions - needed to do cancellations.
    private final HashMap<Session, Subscription> sessionSubscription =
            new HashMap<Session, Subscription>(10);

    synchronized void addSession(Session session, @Nullable RpcTarget target) {
        this.sessionRequest.put(session, target);
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
        LOGGER.log(Level.INFO, "Saving subscription " + this.toString());
        if (this.sessionSubscription.get(session) != null)
            throw new RuntimeException("Subscription already active on this session");
        this.sessionSubscription.put(session, subscription);
    }

    synchronized void removeSubscription(Session session) {
        LOGGER.log(Level.INFO, "Removing subscription " + this.toString());
        this.sessionSubscription.remove(session);
    }

    static {
        LOGGER = Logger.getLogger(RpcObjectManager.class.getName());
        instance = new RpcObjectManager();
        new InitialObjectTarget();  // indirectly registers this object with the RpcObjectManager
    }

    // Map object id to object.
    private final HashMap<String, RpcTarget> objects;

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
            throw new RuntimeException("Object with id " + object.objectId + " already in map");
        LOGGER.log(Level.INFO, "Inserting target " + object.toString());
        this.objects.put(object.objectId, object);
    }

    synchronized RpcTarget getObject(String id) {
        LOGGER.log(Level.INFO, "Getting object " + id);
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

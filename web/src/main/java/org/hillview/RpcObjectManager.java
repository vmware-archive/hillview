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

import org.hillview.targets.InitialObjectTarget;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.RpcTargetAction;
import org.hillview.utils.Utilities;
import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

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
     * This file contains the global properties that control hillview.
     * This file is read by the root node.
     */
    static final String propertiesFile = "hillview.properties";

    /**
     * Well-known id of the initial object.
     */
    static final RpcTarget.Id initialObjectId = RpcTarget.Id.initialId();

    // We have exactly one instance of this object, because the web server
    // is multi-threaded and it instantiates various classes on demand to service requests.
    // These need to be able to find the ObjectManager - they do it through
    // the unique global instance.
    public static final RpcObjectManager instance;

    // Global application properties
    public final Properties properties;

    // Map the session to the targetId object that is replying to the request, if any.
    private final HashMap<Session, RpcTarget> sessionRequest =
            new HashMap<Session, RpcTarget>(10);
    // Mapping sessions to RxJava subscriptions - needed to do cancellations.
    private final HashMap<Session, Subscription> sessionSubscription =
            new HashMap<Session, Subscription>(10);

    private final RedoLog objectLog;

    // Private constructor
    private RpcObjectManager() {
        this.objectLog = new RedoLog();
        this.properties = new Properties();
        try (FileInputStream prop = new FileInputStream(propertiesFile)) {
            this.properties.load(prop);
        } catch (FileNotFoundException ex) {
            HillviewLogger.instance.info("No properties file found", "{0}", propertiesFile);
        } catch (IOException ex) {
            HillviewLogger.instance.error("Error while loading properties from file", ex);
        }
    }

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

    synchronized void addSubscription(RpcRequestContext context, Subscription subscription) {
        if (subscription.isUnsubscribed())
            // The computation may have already finished by the time we get here!
            return;
        Session session = context.session;
        if (session == null)
            return;
        if (context.computation != null)
            // This means that we are replaying the computation; the subscription is
            // probably already saved.
            return;
        HillviewLogger.instance.info("Saving subscription", "{0}:{1}",
                context, subscription);
        if (this.sessionSubscription.get(session) != null)
            // This can happen because we have started the operation, some part of the
            // object was not found on a remote node, and then reconstruction started.
            // The subscription was saved when the operation was initially initiated.
            HillviewLogger.instance.info("Subscription already active on this context",
                    "context={0}", context);
        else
            this.sessionSubscription.put(session, subscription);
    }

    synchronized void removeSubscription(Session session) {
        HillviewLogger.instance.info("Removing subscription", "{0}", this.toString());
        this.sessionSubscription.remove(session);
    }

    static {
        instance = new RpcObjectManager();
        new InitialObjectTarget();  // indirectly registers this object with the RpcObjectManager
    }

    public void addObject(RpcTarget target) {
        HillviewLogger.instance.info("Object generated", "{0} from {1}", target.getId(), target.computation);
        this.objectLog.addObject(target);
    }

    @Nullable RpcTarget getObject(RpcTarget.Id id) {
        return this.objectLog.getObject(id);
    }

    void deleteObject(RpcTarget.Id id) {
        this.objectLog.deleteObject(id);
    }

    /**
     * Execute the specified action.
     */
    @SuppressWarnings("SameParameterValue")
    private void executeAction(RpcTargetAction action, boolean rebuild) {
        RpcTarget target = this.getObject(action.id);
        if (target != null) {
            // Object found
            action.action(target);
            return;
        }
        // Object not found.
        if (rebuild) {
            // Attempt to rebuild the object.
            this.rebuild(action);
        } else {
            throw new RuntimeException("Cannot find object " + action.id);
        }
    }

    public void when(RpcTarget.Id id, Consumer<RpcTarget> consumer) {
        RpcTargetAction append = new RpcTargetAction(id) {
            @Override
            public void action(RpcTarget target) {
                consumer.accept(target);
            }
        };
        this.executeAction(append, true);
    }

    /**
     * Retrieve the RpcTarget with the specified id, when available, pass it to
     * the consumer as an argument for execution.
     * @param id     Id to retrieve.
     * @param consumer Action to execute when the RpcTarget is found.
     */
    public void when(String id, Consumer<RpcTarget> consumer) {
        this.when(new IRpcTarget.Id(id), consumer);
    }

    /**
     * Retrieve all the RpcTargets with the specified ids, when available, pass them to
     * the consumer as an argument for execution.
     * @param ids     Ids to retrieve.
     * @param consumer Action to execute when all the RpcTargets are found.
     */
    public void when(List<String> ids, Consumer<List<RpcTarget>> consumer) {
        if (ids.isEmpty()) {
            consumer.accept(Utilities.list());
        } else {
            String first = ids.remove(0);
            this.when(ids, l -> this.when(first, f -> {
                l.add(0, f);
                consumer.accept(l);
            }));
        }
    }

    /**
     * We have lost the object specified by the action.  Try to reconstruct it
     * from the history and then execute the action.
     * @param action   Action that needs to be executed.
     */
    private void rebuild(RpcTargetAction action) {
        HillviewLogger.instance.info("Attempt to reconstruct", "{0}", action.id);
        HillviewComputation computation = this.objectLog.getComputation(action.id);
        if (computation != null) {
            // The following may trigger a recursive reconstruction.
            HillviewLogger.instance.info("Replaying", "computation={0}", computation);
            computation.replay(action);
        } else {
            RuntimeException ex = new RuntimeException("Don't know how to reconstruct " + action.id);
            HillviewLogger.instance.error("Could not locate computation", ex);
            throw ex;
        }
    }

    /**
     * Removes all RemoteObjects from the cache, except the initial object.
     * @return  The number of objects removed.
     */
    public int removeAllObjects() {
        return this.objectLog.removeAllObjects(initialObjectId);
    }
}

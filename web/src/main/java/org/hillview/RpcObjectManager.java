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
import rx.Observer;
import rx.Subscription;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.HashMap;
import java.util.HashSet;

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
    static final RpcTarget.Id initialObjectId = RpcTarget.Id.initialId();

    // We have exactly one instance of this object, because the web server
    // is multi-threaded and it instantiates various classes on demand to service requests.
    // These need to be able to find the ObjectManager - they do it through
    // the unique global instance.
    public static final RpcObjectManager instance;

    // Map the session to the targetId object that is replying to the request, if any.
    private final HashMap<Session, RpcTarget> sessionRequest =
            new HashMap<Session, RpcTarget>(10);
    // Mapping sessions to RxJava subscriptions - needed to do cancellations.
    private final HashMap<Session, Subscription> sessionSubscription =
            new HashMap<Session, Subscription>(10);

    /**
     * Objects currently under reconstruction.
     */
    private final HashSet<RpcTarget.Id> reconstructing;
    private final RedoLog objectLog;

    // Private constructor
    private RpcObjectManager() {
        this.objectLog = new RedoLog();
        this.reconstructing = new HashSet<RpcTarget.Id>();
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
        HillviewLogger.instance.info("Saving subscription", "{0}", context.toString());
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
        // If we have the object we are definitely not reconstructing it.
        this.reconstructing.remove(target.getId());
    }

    @Nullable RpcTarget getObject(RpcTarget.Id id) {
        return this.objectLog.getObject(id);
    }

    void deleteObject(RpcTarget.Id id) {
        this.objectLog.deleteObject(id);
    }

    /**
     * Attempt to retrieve the object with the specified id.
     * @param id           Object id to retrieve.
     * @param toNotify     An observer notified when the object is retrieved.
     * @param rebuild      If true attempt to rebuild the object if not found.
     */
    public void retrieveTarget(RpcTarget.Id id, boolean rebuild, Observer<RpcTarget> toNotify) {
        RpcTarget target = this.getObject(id);
        if (target != null) {
            // Object found: notify the observer right away.
            toNotify.onNext(target);
            toNotify.onCompleted();
            return;
        }
        // Object not found.
        if (rebuild) {
            // Attempt to rebuild the object.
            this.rebuild(id, toNotify);
        } else {
            toNotify.onError(new RuntimeException("Cannot find object " + id));
        }
    }

    /**
     * We have lost the object with the specified id.  Try to reconstruct it
     * from the history.
     * @param id  Id of object to reconstruct.
     * @param toNotify An observer that is notified when the object is available.
     */
    private void rebuild(RpcTarget.Id id, Observer<RpcTarget> toNotify) {
        HillviewLogger.instance.info("Attempt to reconstruct", "{0}", id);
        if (this.reconstructing.contains(id)) {
            Exception ex = new RuntimeException("Recursive dependences while reconstructing " + id);
            HillviewLogger.instance.error("Recursion in reconstruction", ex);
            toNotify.onError(ex);
            return;
        }
        this.reconstructing.add(id);
        HillviewComputation computation = this.objectLog.getComputation(id);
        if (computation != null) {
            // The following may trigger a recursive reconstruction.
            HillviewLogger.instance.info("Replaying", "computation={0}", computation);
            computation.replay(toNotify);
        } else {
            Exception ex = new RuntimeException("Don't know how to reconstruct " + id);
            HillviewLogger.instance.error("Could not locate computation", ex);
            toNotify.onError(ex);
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

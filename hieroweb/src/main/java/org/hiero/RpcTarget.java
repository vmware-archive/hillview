/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hiero;

import com.google.gson.JsonObject;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.utils.Converters;
import rx.Observer;

import javax.websocket.Session;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class RpcTarget {
    protected String objectId;
    private final HashMap<String, Method> executor;
    protected static final Logger logger = Logger.getLogger(RpcTarget.class.getName());

    RpcTarget() {
        this.executor = new HashMap<String, Method>();
        this.registerExecutors();
        RpcObjectManager.instance.addObject(this);
    }

    public void setId(String objectId) {
        this.objectId = objectId;
    }

    /**
     * Use reflection to register all methods that have an @HieroRpc annotation.
     * These methods will be invoked for each RpcRequest received.
     * All these methods should have the following signature:
     * method(RpcRequest req, Session session).
     * The method is responsible for:
     * - parsing the arguments of the RpcCall
     * - sending the replies, in any number they may be, using the session
     * - closing the session on termination.
     */
    private void registerExecutors() {
        Class<?> type = this.getClass();
        for (Method m : type.getDeclaredMethods()) {
            if (m.isAnnotationPresent(HieroRpc.class)) {
                logger.log(Level.INFO, "Registered RPC method " + m.getName());
                this.executor.put(m.getName(), m);
            }
        }
    }

    /**
     * Dispatches an RPC request for execution.
     * This will look up the method in the RpcRequest using reflection
     * and invoke it using Java reflection.
     */
    public void execute(RpcRequest request, Session session)
            throws InvocationTargetException, IllegalAccessException {
        Method cons = this.executor.get(request.method);
        if (cons == null)
            throw new RuntimeException("No such method " + request.method);
        cons.invoke(this, request, session);
    }

    @Override
    public int hashCode() { return this.objectId.hashCode(); }

    class ResultObserver<T extends IJson> implements Observer<PartialResult<T>> {
        final RpcRequest request;
        final Session session;

        // TODO: handle session disconnections

        ResultObserver(RpcRequest request, Session session) {
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
            logger.log(Level.INFO, "Received partial result");
            if (!this.session.isOpen()) {
                logger.log(Level.WARNING, "Session closed, ignoring partial result");
                return;
            }

            JsonObject json = new JsonObject();
            json.addProperty("done", pr.deltaDone);
            T delta = Converters.checkNull(pr.deltaValue);
            json.add("data", delta.toJsonTree());
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

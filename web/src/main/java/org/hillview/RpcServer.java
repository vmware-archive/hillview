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

package org.hillview;

import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;

import javax.annotation.Nullable;
import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.*;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A server which implements RPC calls between a web browser client and
 * a Java-based web server.  The web server may create a different
 * instance of this class for each request.  The client should
 * send exactly one request for each session; the server may send zero, one
 * or more replies for each session.  The client or server can both
 * choose to close the connection at any time.
 */
@ServerEndpoint(value = "/rpc")
public final class RpcServer {
    static private final int version = 2;
    private static final Logger logger =
            Logger.getLogger(RpcServer.class.getName());

    // Map the session to the target object that is replying, if any
    private static final HashMap<Session, RpcTarget> sessionRequest =
            new HashMap<Session, RpcTarget>(10);

    synchronized private void addSession(Session session, @Nullable RpcTarget target) {
        sessionRequest.put(session, target);
    }

    synchronized private void removeSession(Session session) {
        sessionRequest.remove(session);
    }

    @Nullable synchronized private RpcTarget getTarget(Session session) {
        return sessionRequest.get(session);
    }

    @SuppressWarnings("unused")
    @OnOpen
    public void onOpen(Session session) {
        logger.log(Level.INFO, "Server " + Integer.toString(version) +
                        " new connection with client: {0}",
                session.getId());
        this.addSession(session, null);
    }

    @SuppressWarnings("unused")
    @OnMessage
    public void onMessage(String message, Session session) {
        logger.log(Level.INFO, "New message from Client [{0}]: {1}",
                new Object[] {session.getId(), message});

        RpcRequest req;
        try {
            Reader reader = new StringReader(message);
            JsonReader jReader = new JsonReader(reader);
            JsonElement elem = Streams.parse(jReader);
            req = new RpcRequest(elem);
            if (sessionRequest.get(session) != null)
                throw new RuntimeException("Session already associated with a request!");
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error processing json: ", ex);
            this.replyWithError(ex, session);
            return;
        }

        this.execute(req, session);
    }

    private void sendReply(RpcReply reply, Session session) {
        try {
            JsonElement json = reply.toJson();
            session.getBasicRemote().sendText(json.toString());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Could not send reply");
        }
    }

    static String asString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    private void execute(RpcRequest rpcRequest, Session session) {
        logger.log(Level.INFO, "Executing " + rpcRequest.toString());
        try {
            RpcTarget target = RpcObjectManager.instance.getObject(rpcRequest.objectId);
            this.addSession(session, target);
            // This function is responsible for sending the replies and closing the session.
            target.execute(rpcRequest, session);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Return exception ", ex);
            RpcReply reply = rpcRequest.createReply(ex);
            this.sendReply(reply, session);
            rpcRequest.syncCloseSession(session);
        }
    }

    private void closeSession(final Session session) {
        try {
            if (session.isOpen())
                session.close();
            this.removeSession(session);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error closing session");
        }
    }

    private void replyWithError(final Throwable th, final Session session) {
        final RpcReply reply = new RpcReply(-1, asString(th), true);
        this.sendReply(reply, session);
        this.closeSession(session);
    }

    @SuppressWarnings("unused")
    @OnClose
    public void onClose(final Session session, final CloseReason reason) {
        RpcTarget target = this.getTarget(session);
        if (target != null)
            target.cancel();
        if (reason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE)
            logger.log(Level.SEVERE, "Close connection for client: {0}, {1}",
                    new Object[] { session.getId(), reason.toString() });
        else
            logger.log(Level.INFO, "Normal connection closing for client: {0}",
                    new Object[] { session.getId() });
        this.removeSession(session);
    }

    @SuppressWarnings("unused")
    @OnError
    public void onError(final Throwable exception, final Session unused) {
        logger.log(Level.SEVERE, "Error: ", exception);
    }
}

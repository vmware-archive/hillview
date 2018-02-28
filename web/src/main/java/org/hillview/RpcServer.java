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

import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;
import rx.Subscription;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.InflaterInputStream;

/**
 * A server which implements RPC calls between a web browser client and
 * a Java-based web server.  The web server may create a different
 * instance of this class for each request.  The client should
 * send exactly one request for each session; the server may send zero, one
 * or more replies for each session.  The client or server can both
 * choose to close the connection at any time.  This class must be public.
 */
@SuppressWarnings("WeakerAccess")
@ServerEndpoint(value = "/rpc")
public final class RpcServer {
    static private final int version = 2;
    /**
     * There seems to be a significant but in RxJava: when onComplete is called,
     * it may kill the consumer thread, even if that thread may not have finished
     * processing the previous onNext.  So we use a different thread to do
     * the actual message sending.  This thread pool must have exactly 1 thread,
     * because we want all rpc replies to be sent in the same order as they are prepared.
     */
    private static final ExecutorService replyExecutor = Executors.newFixedThreadPool(1);

    private static void sendReply(RpcReply reply, Session session) {
        replyExecutor.execute(() -> {
            try {
                JsonElement json = reply.toJson();
                session.getBasicRemote().sendText(json.toString());
            } catch (Exception e) {
                HillviewLogger.instance.error("Could not send reply", e);
            }
        });
    }

    public static void closeSession(final Session session) {
        // We use replyExecutor to make sure that the session closing
        // is performed after all replies on that session have been sent.
        replyExecutor.execute( () -> {
            try {
                if (session.isOpen())
                    session.close();
                RpcObjectManager.instance.removeSession(session);
            } catch (Exception ex) {
                HillviewLogger.instance.error("Error closing context", ex);
            }
        });
    }

    @SuppressWarnings("unused")
    @OnOpen
    public void onOpen(Session session) {
        HillviewLogger.instance.info(
                "New connection with client", "version {0} client {1}",
                version, session.getId());
        RpcObjectManager.instance.addSession(session, null);
    }

    @SuppressWarnings("unused")
    @OnMessage
    public void onMessage(ByteBuffer message, Session session) {
        HillviewLogger.instance.info("New message from client",
                "{0}", session.getId());
        RpcRequest req;
        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(message.array());
            InflaterInputStream decompressed = new InflaterInputStream(stream);
            Reader reader = new InputStreamReader(decompressed);
            JsonReader jReader = new JsonReader(reader);
            JsonElement elem = Streams.parse(jReader);
            HillviewLogger.instance.info("Decoded message", "{0}", elem.toString());
            req = new RpcRequest(elem);
        } catch (Exception ex) {
            HillviewLogger.instance.error("Error processing json", ex);
            this.replyWithError(ex, session);
            return;
        }

        if (RpcObjectManager.instance.getTarget(session) != null)
            throw new RuntimeException("Session already associated with a request!");
        RpcRequestContext context = new RpcRequestContext(session);
        RpcServer.execute(req, context);
    }

    public static void execute(RpcRequest rpcRequest, RpcRequestContext context) {
        HillviewLogger.instance.info("Executing request", "{0}", rpcRequest);
        /* Observable invoked when the source object has been obtained.
         * This observable will actually invoke the method on the object. */
        SingleObserver<RpcTarget> obs = new SingleObserver<RpcTarget>() {
            @Override
            public void onError(Throwable throwable) {
                HillviewLogger.instance.error("Return exception", throwable);
                RpcReply reply = rpcRequest.createReply(throwable);
                Session session = context.getSessionIfOpen();
                if (session == null)
                    return;
                RpcServer.sendReply(reply, session);
                rpcRequest.syncCloseSession(session);
            }

            @Override
            public void onSuccess(RpcTarget rpcTarget) {
                if (context.session != null)
                    RpcObjectManager.instance.addSession(context.session, rpcTarget);
                try {
                    // This function is responsible for sending the replies and closing the session.
                    rpcTarget.execute(rpcRequest, context);
                } catch (Exception ex) {
                    this.onError(ex);
                }
            }
        };
        // Retrieve the source object on which the operation is executed.
        // This works asynchronously - when the object is retrieved obs is invoked.
        RpcObjectManager.instance.retrieveTarget(rpcRequest.objectId, true, obs);
    }

    private void replyWithError(final Throwable th, final Session session) {
        final RpcReply reply = new RpcReply(
                -1, Converters.checkNull(Utilities.throwableToString(th)), true);
        RpcServer.sendReply(reply, session);
        RpcServer.closeSession(session);
    }

    @SuppressWarnings("unused")
    @OnClose
    public void onClose(final Session session, final CloseReason reason) {
        if (reason.getCloseCode() != CloseReason.CloseCodes.NORMAL_CLOSURE) {
            HillviewLogger.instance.error("Abnormal close of connection for client",
                    "{0}, {1}", session.getId(), reason.toString());
            HillviewLogger.instance.error("Stack trace from exception", new Exception());
        } else {
            HillviewLogger.instance.info("Normal connection closing for client",
                    "{0}", session.getId());
        }
        Subscription sub = RpcObjectManager.instance.getSubscription(session);
        if (sub != null) {
            HillviewLogger.instance.info("Unsubscribing", "{0}", this.toString());
            sub.unsubscribe();
            RpcObjectManager.instance.removeSubscription(session);
        }
        RpcObjectManager.instance.removeSession(session);
    }

    @SuppressWarnings("unused")
    @OnError
    public void onError(final Throwable exception, final Session unused) {
        HillviewLogger.instance.error("Exception", exception);
    }
}

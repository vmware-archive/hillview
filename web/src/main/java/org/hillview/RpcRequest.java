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
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.util.logging.Level;
import java.util.logging.Logger;

final class RpcRequest {
    private static final Logger LOGGER =
            Logger.getLogger(RpcRequest.class.getName());

    private final int requestId;
    final String objectId;
    public final String method;
    @Nullable
    private final String arguments;  // A JSON string

    public RpcRequest(JsonElement element) {
        final JsonObject obj = element.getAsJsonObject();
        this.requestId = obj.get("requestId").getAsInt();
        this.objectId = obj.get("objectId").getAsString();
        this.method = obj.get("method").getAsString();
        this.arguments = obj.get("arguments").getAsString();
    }

    @Override
    public String toString() {
        return "RpcRequest: " + this.objectId + "." + this.method + "()";
    }

    RpcReply createReply(IJson userResult) {
        return this.createReply(userResult.toJsonTree());
    }

    private RpcReply createReply(String json) {
        return new RpcReply(this.requestId, json, false);
    }

    RpcReply createReply(JsonElement userResult) {
        return this.createReply(userResult.toString());
    }

    RpcReply createReply(Throwable th) {
        return new RpcReply(this.requestId, this.toString() + "\n" + RpcServer.asString(th), true);
    }

    <T> T parseArgs(Class<T> classOfT) {
        return IJson.gsonInstance.fromJson(this.arguments, classOfT);
    }

    /**
     * Initiated by the server.
     * @param session  Session to close.
     */
    void syncCloseSession(Session session) {
        try {
            session.close();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error closing session");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        RpcRequest that = (RpcRequest) o;
        if (this.requestId != that.requestId) return false;
        if (!this.objectId.equals(that.objectId)) return false;
        if (!this.method.equals(that.method)) return false;
        return this.arguments != null ? this.arguments.equals(that.arguments) : that.arguments == null;
    }

    @Override
    public int hashCode() {
        int result = this.requestId;
        result = 31 * result + this.objectId.hashCode();
        result = 31 * result + this.method.hashCode();
        result = 31 * result + (this.arguments != null ? this.arguments.hashCode() : 0);
        return result;
    }
}

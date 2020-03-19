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
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import javax.websocket.Session;

public final class RpcRequest implements IJson {
    static final long serialVersionUID = 1;

    /**
     * Each pair request-response uses a matching id.
     */
    private final int requestId;
    /**
     * The id of the RpcTarget on which this RpcRequest operates.
     */
    final RpcTarget.Id objectId;
    /**
     * The method of the RpcTarget that is being invoked.
     */
    public final String method;
    /**
     * The method arguments, encoded as JSON; the method will decode these itself.
     */
    @Nullable
    private final String arguments;
    /**
     * Original encoding of the request as JSON.
     */
    private final JsonElement element;

    public RpcRequest(JsonElement element) {
        final JsonObject obj = element.getAsJsonObject();
        this.element = element;
        this.requestId = obj.get("requestId").getAsInt();
        this.objectId = new RpcTarget.Id(obj.get("objectId").getAsString());
        this.method = obj.get("method").getAsString();
        this.arguments = obj.get("arguments").getAsString();
    }

    /**
     * Returns the ids of the RpcTarget objects that store datasets to which
     * method is applied.  For most methods this returns this.requestId,
     * but for "zip" methods this will return a two-element array.  This method
     * relies on the JSON method corresponding to IDataSet.zip calls being
     * also called "zip".
     */
    RpcTarget.Id[] getDatasetSourceIds() {
        if (this.method.equals("zip")) {
            String otherId = this.parseArgs(String.class);
            return new RpcTarget.Id[] { this.objectId, new RpcTarget.Id(otherId) };
        }
        return new RpcTarget.Id[] { this.objectId };
    }

    @Override
    public String toString() {
        return this.objectId + "." + this.method + "()";
    }

    private RpcReply createReply(String json) {
        return new RpcReply(this.requestId, json, false);
    }

    public RpcReply createReply(JsonElement userResult) {
        return this.createReply(userResult.toString());
    }

    RpcReply createReply(Throwable th) {
        return new RpcReply(this.requestId, this.toString() + "\n" +
                Utilities.throwableToString(th), true);
    }

    RpcReply createCompletedReply() {
        return new RpcReply(this.requestId);
    }

    public <T> T parseArgs(Class<T> classOfT) {
        return IJson.gsonInstance.fromJson(this.arguments, classOfT);
    }

    /**
     * Initiated by the server.
     * @param session  Session to close.
     */
    public void syncCloseSession(@Nullable Session session) {
        if (session == null)
            return;
        RpcServer.closeSession(session);
    }

    @Override
    public JsonElement toJsonTree() {
        return this.element;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        RpcRequest that = (RpcRequest) o;
        return this.requestId == that.requestId &&
                this.objectId.equals(that.objectId) &&
                this.method.equals(that.method) &&
                (this.arguments != null ? this.arguments.equals(that.arguments) : that.arguments == null);
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

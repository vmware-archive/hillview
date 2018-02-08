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
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.IOException;

/**
 * Represents a reply that is sent from the web server to the web client.
 */
public final class RpcReply {
    /**
     * Request that is being answered.
     */
    private final int requestId;
    /**
     * Actual result: a string encoding a json object.
     */
    private final String result;
    /**
     * True if this reply represents an error that occurred.
     */
    private final boolean isError;

    RpcReply(final int requestId, final String result, boolean isError) {
        this.requestId = requestId;
        this.result = result;
        this.isError = isError;
    }

    JsonElement toJson() {
        JsonObject result = new JsonObject();
        result.addProperty("requestId", this.requestId);
        result.addProperty("result", this.result);
        result.addProperty("isError", this.isError);
        return result;
    }

    @Override
    public String toString() {
        return "RpcReply to " + this.requestId + ": " +
                (this.isError ? "Error" : "Normal") +
                "Message: " + Utilities.truncateString(this.result);
    }

    /**
     * Send a reply using the specified web sockets context.
     * @param session If the context is null no reply is sent.
     */
    public void send(@Nullable Session session) {
        HillviewLogger.instance.info("Sending reply", "{0}", this);
        try {
            if (session == null) {
                HillviewLogger.instance.info("No session; reply skipped.");
                return;
            }
            JsonElement json = this.toJson();
            session.getBasicRemote().sendText(json.toString());
            HillviewLogger.instance.info("Reply sent");
        } catch (IOException e) {
            HillviewLogger.instance.error("Could not send reply", e);
        }
    }
}

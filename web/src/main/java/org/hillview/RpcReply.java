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
import org.hillview.utils.HillviewLogging;

import javax.annotation.Nullable;
import javax.websocket.Session;
import java.io.IOException;

class RpcReply {
    private final int requestId;
    private final String result;
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

    /**
     * Send a reply using the specified web sockets context.
     * @param session If the context is null no reply is sent.
     */
    void send(@Nullable Session session) {
        try {
            if (session == null) {
                HillviewLogging.logger().info("No context; reply skipped.");
                return;
            }
            JsonElement json = this.toJson();
            session.getBasicRemote().sendText(json.toString());
            HillviewLogging.logger().info("Reply sent");
        } catch (IOException e) {
            HillviewLogging.logger().info("Could not send reply");
        }
    }
}

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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import javax.websocket.Session;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RpcReply {
    private final int requestId;
    private final String result;
    private final boolean isError;
    private static final Logger logger =
            Logger.getLogger(RpcReply.class.getName());

    public RpcReply(final int requestId, final String result, boolean isError) {
        this.requestId = requestId;
        this.result = result;
        this.isError = isError;
    }

    public JsonElement toJson() {
        JsonObject result = new JsonObject();
        result.addProperty("requestId", this.requestId);
        result.addProperty("result", this.result);
        result.addProperty("isError", this.isError);
        return result;
    }

    public void send(Session session) {
        try {
            JsonElement json = this.toJson();
            session.getBasicRemote().sendText(json.toString());
            RpcReply.logger.log(Level.INFO, "Reply sent");
        } catch (IOException e) {
            RpcReply.logger.log(Level.SEVERE, "Could not send reply");
        }
    }
}

/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import com.google.common.net.HostAndPort;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.hillview.dataset.api.IJson;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Describes the list of hosts that comprise a cluster. The corresponding Json representation
 * would be:
 *          {
 *              "serverList": ["127.0.0.1:1234", "127.0.0.1:1235"]
 *          }
 */
public final class ClusterDescription implements IJson {
    private final List<HostAndPort> serverList;

    public ClusterDescription(final List<HostAndPort> serverList) {
        this.serverList = serverList;
    }

    public List<HostAndPort> getServerList() {
        return this.serverList;
    }

    public static class HostAndPortSerializer implements JsonSerializer<HostAndPort> {
        public JsonElement serialize(HostAndPort hostAndPort, Type typeOfSchema,
                                     JsonSerializationContext context) {
            return new JsonPrimitive(hostAndPort.toString());
        }
    }

    public static class HostAndPortDeserializer implements JsonDeserializer<HostAndPort> {
        public HostAndPort deserialize(JsonElement json, Type typeOfT,
                                  JsonDeserializationContext context)
                throws JsonParseException {
            return HostAndPort.fromString(json.getAsString());
        }
    }
}
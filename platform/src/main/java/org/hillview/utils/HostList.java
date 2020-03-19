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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.hillview.dataset.api.IJson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Describes the list of hosts running the hillview service.
 * The corresponding Json representation is:
 *          {
 *              "serverList": ["192.168.0.1:1234", "192.168.0.2:1234"]
 *          }
 */
public final class HostList implements IJson {
    static final long serialVersionUID = 1;

    private final List<HostAndPort> serverList;

    public int size() { return this.serverList.size(); }

    public HostList(final List<HostAndPort> serverList) {
        this.serverList = serverList;
    }

    public List<HostAndPort> getServerList() {
        return this.serverList;
    }

    public HostAndPort get(int index) {
        return this.serverList.get(index);
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

    public static HostList fromFile(String filename) throws IOException {
        final List<String> lines = Files.readAllLines(Paths.get(filename), Charset.defaultCharset());
        final List<HostAndPort> hostAndPorts = lines.stream()
                .map(HostAndPort::fromString)
                .collect(Collectors.toList());
        return new HostList(hostAndPorts);
    }
}

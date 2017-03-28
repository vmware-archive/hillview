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
 *
 */

package org.hiero.sketch.dataset.api;

import com.google.gson.*;
import org.hiero.sketch.table.Schema;
import org.hiero.utils.Converters;

import java.lang.reflect.Type;
import java.time.LocalDateTime;

// Unfortunately this module introduces many circular dependences, because it has
// to register various type adaptors.

public interface IJson {
    class DateSerializer implements JsonSerializer<LocalDateTime> {
        public JsonElement serialize(LocalDateTime data, Type typeOfSchema, JsonSerializationContext context) {
            double d = Converters.toDouble(data);
            return new JsonPrimitive(d);
        }
    }

    // Use these instances for all your json serialization needs
    GsonBuilder builder = new GsonBuilder()
        .registerTypeAdapter(Schema.class, new Schema.SchemaSerializer())
        .registerTypeAdapter(Schema.class, new Schema.SchemaDeserializer())
        .registerTypeAdapter(LocalDateTime.class, new DateSerializer());
    Gson gsonInstance = builder.serializeNulls().create();

    /**
     * Default JSON string representation of this.
     * @return The JSON representation of this.
     */
    default String toJson() {
        JsonElement t = this.toJsonTree();
        return t.toString();
    }

    /**
     * Override this method if needed to implement the Json serialization.
     * @return The serialized representation of this.
     */
    default JsonElement toJsonTree() { return gsonInstance.toJsonTree(this); }
}

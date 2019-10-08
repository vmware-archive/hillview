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

package org.hillview.dataset.api;

import com.google.gson.*;
import org.hillview.table.columns.ColumnPrivacyMetadata;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.table.columns.IntColumnPrivacyMetadata;
import org.hillview.table.columns.StringColumnPrivacyMetadata;
import org.hillview.utils.HostList;
import org.hillview.sketches.results.NextKList;
import org.hillview.table.Schema;
import org.hillview.utils.Converters;
import org.hillview.utils.HostAndPort;
import org.hillview.utils.RuntimeTypeAdapterFactory;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.time.Instant;

// Unfortunately this module introduces many circular dependencies, because it has
// to register various type adaptors.

public interface IJson extends Serializable {
    class DateSerializer implements JsonSerializer<Instant> {
        public JsonElement serialize(Instant data, Type typeOfSchema, JsonSerializationContext
                unused) {
            double d = Converters.toDouble(data);
            return new JsonPrimitive(d);
        }
    }

    class DateDeserializer implements JsonDeserializer<Instant> {
        public Instant deserialize(JsonElement data, Type typeOfSchema, JsonDeserializationContext
                unused) {
            double d = data.getAsDouble();
            return Converters.toDate(d);
        }
    }

    class NextKSerializer
            implements JsonSerializer<NextKList> {
        public JsonElement serialize(NextKList data, Type typeOfSchema, JsonSerializationContext unused) {
            return data.toJsonTree();
        }
    }

    // Use these instances for all your json serialization needs
    GsonBuilder builder = new GsonBuilder()
            .registerTypeAdapter(Schema.class, new Schema.Serializer())
            .registerTypeAdapter(Schema.class, new Schema.Deserializer())
            .registerTypeAdapter(NextKList.class, new NextKSerializer())
            .registerTypeAdapter(Instant.class, new DateSerializer())
            .registerTypeAdapter(HostAndPort.class, new HostList.HostAndPortSerializer())
            .registerTypeAdapter(HostAndPort.class, new HostList.HostAndPortDeserializer())
            .registerTypeAdapterFactory(
                    RuntimeTypeAdapterFactory.of(ColumnPrivacyMetadata.class)
                            .registerSubtype(ColumnPrivacyMetadata.class)
                            .registerSubtype(DoubleColumnPrivacyMetadata.class)
                            .registerSubtype(IntColumnPrivacyMetadata.class)
                            .registerSubtype(StringColumnPrivacyMetadata.class))
            ;

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

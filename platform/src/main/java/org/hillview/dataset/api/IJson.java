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
import org.hillview.sketches.results.Count;
import org.hillview.table.api.Interval;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.HostList;
import org.hillview.sketches.results.NextKList;
import org.hillview.table.Schema;
import org.hillview.utils.HostAndPort;
import org.hillview.utils.RuntimeTypeAdapterFactory;

import java.io.Serializable;
import java.lang.reflect.Type;

// Unfortunately this module introduces many circular dependencies, because it has
// to register various type adaptors.

public interface IJson extends Serializable {
    class IntervalSerializer implements JsonSerializer<Interval> {
        @Override
        public JsonElement serialize(Interval interval, Type type, JsonSerializationContext jsonSerializationContext) {
            JsonArray result = new JsonArray();
            result.add(interval.get(true));
            result.add(interval.get(false));
            return result;
        }
    }

    class IntervalDeserializer implements JsonDeserializer<Interval> {
        @Override
        public Interval deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            double start = jsonElement.getAsJsonArray().get(0).getAsDouble();
            double end = jsonElement.getAsJsonArray().get(1).getAsDouble();
            return new Interval(start, end);
        }
    }

    class CountSerializer implements JsonSerializer<Count> {
        @Override
        public JsonElement serialize(Count count, Type type, JsonSerializationContext jsonSerializationContext) {
            return count.toJsonTree();
        }
    }

    class NextKSerializer
            implements JsonSerializer<NextKList> {
        public JsonElement serialize(NextKList data, Type typeOfSchema, JsonSerializationContext unused) {
            return data.toJsonTree();
        }
    }

    class RowSnapshotSerializer implements JsonSerializer<RowSnapshot> {
        @Override
        public JsonElement serialize(RowSnapshot rowSnapshot, Type type, JsonSerializationContext jsonSerializationContext) {
            return rowSnapshot.toJsonTree();
        }
    }

    // Use these instances for all your json serialization needs
    GsonBuilder builder = new GsonBuilder()
            .registerTypeAdapter(Schema.class, new Schema.Serializer())
            .registerTypeAdapter(Schema.class, new Schema.Deserializer())
            .registerTypeAdapter(RowSnapshot.class, new RowSnapshotSerializer())
            .registerTypeAdapter(NextKList.class, new NextKSerializer())
            .registerTypeAdapter(Count.class, new CountSerializer())
            .registerTypeAdapter(Interval.class, new IntervalSerializer())
            .registerTypeAdapter(Interval.class, new IntervalDeserializer())
            .registerTypeAdapter(HostAndPort.class, new HostList.HostAndPortSerializer())
            .registerTypeAdapter(HostAndPort.class, new HostList.HostAndPortDeserializer())
            .registerTypeAdapterFactory(
                    RuntimeTypeAdapterFactory.of(ColumnQuantization.class)
                            .registerSubtype(ColumnQuantization.class)
                            .registerSubtype(DoubleColumnQuantization.class)
                            .registerSubtype(StringColumnQuantization.class))
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

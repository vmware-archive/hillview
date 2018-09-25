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

package org.hillview.storage;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * Reads data from a file containing data encoded as JSON.
 * The assumed format is as follows:
 * - the file contains a single JSON array
 * - the array elements are flat JSON objects
 * - each value will become a row in the table
 * - all JSON objects have the same structure (schema)
 * - JSON objects generate a column for each property
 * TODO: add support for sparse data, with different schemas and hierarchical objects.
 * TODO: add support for streaming reads
 */
public class JsonFileLoader extends TextFileLoader {
    @Nullable
    private final String schemaPath;

    public JsonFileLoader(String filename, @Nullable String schemaPath) {
        super(filename);
        this.schemaPath = schemaPath;
        this.currentRow = 0;
        this.currentColumn = -1;
    }

    public ITable load() {
        Schema schema = null;
        if (this.schemaPath != null)
            schema = Schema.readFromJsonFile(Paths.get(this.schemaPath));

        Reader file = this.getFileReader();
        JsonReader jReader = new JsonReader(file);
        JsonElement elem = Streams.parse(jReader);
        if (!elem.isJsonArray())
            throw new RuntimeException("Expected a JSON array in " + filename);
        JsonArray array = elem.getAsJsonArray();
        if (array.size() == 0 && schema == null)
            throw new RuntimeException("Empty JSON array in " + filename);

        if (schema == null)
            schema = this.guessSchema(filename, array.iterator());
        IAppendableColumn[] columns = schema.createAppendableColumns();

        this.currentRow = 0;
        for (JsonElement e : array)
            this.append(columns, e);

        this.close(file);
        return new Table(columns, this.filename, null);
    }

    private static ContentsKind getKind(@Nullable JsonElement e) {
        if (e == null || e.isJsonNull())
            return ContentsKind.None;
        if (e.isJsonArray() || e.isJsonObject())
            throw new RuntimeException("Values must be simple " + e);
        JsonPrimitive prim = e.getAsJsonPrimitive();
        if (prim.isBoolean())
            return ContentsKind.String;
        if (prim.isNumber())
            return ContentsKind.Double;
        if (prim.isString())
            return ContentsKind.String;
        throw new RuntimeException("Unexpected JSON value " + prim);
    }

    Schema guessSchema(String filename, Iterator<JsonElement> collection) {
        Map<String, ContentsKind> colKind = new LinkedHashMap<String, ContentsKind>();
        Set<String> unknownColumns = new HashSet<String>();

        if (!collection.hasNext())
            throw new RuntimeException("Empty json collection in " + filename);

        // Try to guess schema based on first element
        JsonElement el = collection.next();
        if (!el.isJsonObject())
            throw new RuntimeException("Expected a JSON array of JSON objects " + filename);
        JsonObject object = el.getAsJsonObject();

        for (Map.Entry<String, JsonElement> e : object.entrySet()) {
            String name = e.getKey();
            JsonElement value = e.getValue();
            if (value.isJsonArray() || value.isJsonObject())
                throw new RuntimeException("Values must be simple " + value);
            ContentsKind kind = JsonFileLoader.getKind(value);
            if (kind == ContentsKind.None)
                unknownColumns.add(name);
            colKind.put(name, kind);
        }

        // If we could not guess schema scan the rest of the elements.
        this.currentRow = 1;
        List<String> found = new ArrayList<String>();
        while (unknownColumns.size() != 0 && collection.hasNext()) {
            el = collection.next();
            this.currentRow++;
            if (!el.isJsonObject())
                this.error("Expected a JSON array of JSON objects");
            object = el.getAsJsonObject();

            for (String u: unknownColumns) {
                JsonElement e = object.get(u);
                ContentsKind k = JsonFileLoader.getKind(e);
                if (k != ContentsKind.None) {
                    found.add(u);
                    colKind.put(u, k);
                }
            }
            unknownColumns.removeAll(found);
            found.clear();
        }

        Schema schema = new Schema();
        for (Map.Entry<String, ContentsKind> e: colKind.entrySet()) {
            ContentsKind kind = e.getValue();
            if (kind == ContentsKind.None)
                // This column is always null
                kind = ContentsKind.String;
            ColumnDescription desc = new ColumnDescription(e.getKey(), kind);
            schema.append(desc);
        }
        return schema;
    }

    void append(IAppendableColumn[] columns, JsonElement e) {
        if (!e.isJsonObject())
            this.error("JSON array element is not a JsonObject");
        JsonObject obj = e.getAsJsonObject();
        for (IAppendableColumn col: columns) {
            JsonElement el = obj.get(col.getName());
            if (el == null || el.isJsonNull()) {
                col.appendMissing();
                continue;
            }

            if (!el.isJsonPrimitive())
                this.error("JSON array element is a non-primitive field");
            JsonPrimitive prim = el.getAsJsonPrimitive();

            if (prim.isBoolean()) {
                col.append(prim.getAsBoolean() ? "true" : "false");
            } else if (prim.isNumber()) {
                col.append(prim.getAsDouble());
            } else if (prim.isString()) {
                col.parseAndAppendString(prim.getAsString());
            } else {
                this.error("Unexpected Json value" + prim.toString());
            }
        }
    }
}

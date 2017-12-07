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
import java.util.Map;

/**
 * Reads data from a file containing data encoded as JSON.
 * The assumed format is as follows:
 * - the file contains a single JSON array
 * - the array elements are flat JSON objects
 * - each value will become a row in the table
 * - all JSON objects have the same structure (schema)
 * - JSON objects generate a column for each property
 * TODO: add support for sparse data, with different schemas and hierarchical objects.
 * TODO: add suport for streaming reads
 */
public class JsonFileLoader extends TextFileLoader {
    @Nullable
    protected String schemaPath;

    public JsonFileLoader(String filename, @Nullable String schemaPath) {
        super(filename);
        this.schemaPath = schemaPath;
    }

    public ITable load() {
        Schema schema = null;
        if (this.schemaPath != null)
            schema = Schema.readFromJsonFile(Paths.get(this.schemaPath));

        Reader file = this.getFileReader();
        JsonReader jReader = new JsonReader(file);
        JsonElement elem = Streams.parse(jReader);
        if (!elem.isJsonArray())
            throw new RuntimeException("Expected a JSON array in file " + filename);
        JsonArray array = elem.getAsJsonArray();
        if (array.size() == 0)
            throw new RuntimeException("Empty JSON array in file " + filename);

        JsonElement el = array.get(0);
        if (schema == null)
            schema = this.guessSchema(filename, el);
        IAppendableColumn[] columns = schema.createAppendableColumns();

        int row = 0;
        for (JsonElement e : array)
            this.append(columns, e, row++);

        return new Table(columns);
    }

    Schema guessSchema(String filename, JsonElement el) {
        if (!el.isJsonObject())
            throw new RuntimeException("Expected a JSON array of JSON objects " + filename);
        JsonObject object = el.getAsJsonObject();
        Schema schema = new Schema();
        int index = 0;
        for (Map.Entry<String, JsonElement> e: object.entrySet()) {
            String name = e.getKey();
            JsonElement value = e.getValue();
            if (value.isJsonArray() || value.isJsonObject())
                throw new RuntimeException("Values must be simple " + value);
            if (value.isJsonNull())
                throw new RuntimeException("Null value in first row");
            JsonPrimitive prim = value.getAsJsonPrimitive();
            ContentsKind kind;
            if (prim.isBoolean())
                kind = ContentsKind.Category;
            else if (prim.isNumber())
                kind = ContentsKind.Double;
            else if (prim.isString())
                kind = ContentsKind.String;
            else
                throw new RuntimeException("Unexpected JSON value " + prim);
            ColumnDescription desc = new ColumnDescription(name, kind, true);
            schema.append(desc);
        }
        return schema;
    }

    void append(IAppendableColumn[] columns, JsonElement e, int row) {
        if (!e.isJsonObject())
            throw new RuntimeException("Row " + row + " is not a JsonObject");
        JsonObject obj = e.getAsJsonObject();
        for (IAppendableColumn col: columns) {
            JsonElement el = obj.get(col.getName());
            if (el == null || el.isJsonNull()) {
                col.appendMissing();
                continue;
            }

            if (!el.isJsonPrimitive())
                throw new RuntimeException("Row " + row + " contains a non-primitive field");
            JsonPrimitive prim = el.getAsJsonPrimitive();

            if (prim.isBoolean()) {
                col.append(prim.getAsBoolean() ? "true" : "false");
            } else if (prim.isNumber()) {
                col.append(prim.getAsDouble());
            } else if (prim.isString()) {
                col.append(prim.getAsString());
            } else {
                throw new RuntimeException("Unexpected Json value on row " + row);
            }
        }
    }
}

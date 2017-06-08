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

package org.hiero.table;

import com.google.gson.*;
import org.hiero.dataset.api.IJson;
import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.ISubSchema;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidParameterException;
import java.util.*;

/**
 * A schema is an ordering of the columns, plus a map from a column name to a column description.
 * Column names are case-sensitive.
 */
public final class Schema
        implements Serializable, IJson {
    private final LinkedHashMap<String, ColumnDescription> columns;
    private final List<String> colNames;

    public static class Serializer implements JsonSerializer<Schema> {
        public JsonElement serialize(Schema schema, Type typeOfSchema,
                                     JsonSerializationContext context) {
            JsonArray result = new JsonArray();
            for (String col: schema.columns.keySet()) {
                ColumnDescription cd = schema.getDescription(col);
                result.add(cd.toJsonTree());
            }
            return result;
        }
    }

    public static class Deserializer implements JsonDeserializer<Schema> {
        public Schema deserialize(JsonElement json, Type typeOfT,
                                  JsonDeserializationContext context)
                throws JsonParseException {
            Schema result = new Schema();
            for (JsonElement e: json.getAsJsonArray()) {
                ColumnDescription cd = gsonInstance.fromJson(e, ColumnDescription.class);
                result.append(cd);
            }
            return result;
        }
    }

    public Schema() {
        this.columns = new LinkedHashMap<String, ColumnDescription>();
        this.colNames = new ArrayList<>();
    }

    public void append(final ColumnDescription desc) {
        if (this.columns.containsKey(desc.name))
            throw new InvalidParameterException("Column with name " +
                    desc.name + " already exists");
        this.columns.put(desc.name, desc);
        this.colNames.add(desc.name);
    }

    public String getColName(int colNum) {
        return this.colNames.get(colNum);
    }

    public ColumnDescription getDescription(final String columnName) {
        return this.columns.get(columnName);
    }

    public ColumnDescription getDescription(final int colNum) {
        return this.columns.get(this.getColName(colNum));
    }

    public int getColumnCount() {
        return this.columns.size();
    }

    public Set<String> getColumnNames() {
        return this.columns.keySet();
    }

    /**
     * Generates a new Schema that contains only the subset of columns contained in the subSchema.
     */
    public Schema project(final ISubSchema subSchema) {
        final Schema projection = new Schema();
        this.columns.values().stream().filter(cd -> subSchema.isColumnPresent(cd.name)).forEach(projection::append);
        return projection;
    }

    /**
     * Generates a column name not already in the schema, starting from the supplied prefix.
     */
    public String newColumnName(@Nullable String prefix) {
        if (prefix == null)
            prefix = "Column";
        int i = 0;
        String name = prefix;
        while (true) {
            if (!this.columns.containsKey(name))
                return name;
            name = prefix + Integer.toString(i);
            ++i;
        }
    }

    @Override
    public String toString() {
        String result = "";
        String separator = "";
        for (ColumnDescription c: this.columns.values()) {
            result += separator + c.toString();
            separator = ", ";
        }
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        final Schema schema = (Schema) o;
        return this.columns.equals(schema.columns);
    }

    @Override
    public int hashCode() {
        return this.columns.hashCode();
    }

    public ContentsKind getKind(final String colName){
        return this.getDescription(colName).kind;
    }

    public static Schema fromJson(String json) {
        return IJson.gsonInstance.fromJson(json, Schema.class);
    }

    public static Schema readFromJsonFile(Path file) throws IOException {
        String s = new String(Files.readAllBytes(file));
        return Schema.fromJson(s);
    }

    public void writeToJsonFile(Path file) throws IOException {
        String text = this.toJson();
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}

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

package org.hillview.table;

import com.google.gson.*;
import org.hillview.dataset.api.IJson;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ISubSchema;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
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
    // Read below about how these variables are mutated even
    // if the schema is supposed to be immutable.
    // cachedColumnNames is also used as a boolean flag.
    @Nullable
    private String[] cachedColumnNames;
    @Nullable
    private ContentsKind[] cachedKinds;

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
                result.append(Converters.checkNull(cd));
            }
            return result;
        }
    }

    public Schema() {
        this.columns = new LinkedHashMap<String, ColumnDescription>();
        this.cachedColumnNames = null;
        this.cachedKinds = null;
    }

    public void append(final ColumnDescription desc) {
        desc.validate();
        if (this.columns.containsKey(desc.name))
            throw new InvalidParameterException("Column with name " +
                    desc.name + " already exists");
        this.columns.put(desc.name, desc);
        if (this.cachedColumnNames != null)
            throw new RuntimeException("Changing immutable schema");
    }

    public int getColumnIndex(String columnName) {
        int index = 0;
        for (String c: this.columns.keySet()) {
            if (c.equals(columnName))
                break;
            index++;
        }
        return index;
    }

    public ColumnDescription getDescription(final String columnName) {
        return this.columns.get(columnName);
    }

    public int getColumnCount() {
        return this.columns.size();
    }

    /**
     * This method is tricky: schemas are supposed to be immutable,
     * but this method mutates the schema.  This must be thread-safe.
     * We rely on the users of the cached data in properly calling seal().
     */
    private synchronized void seal() {
        if (this.cachedColumnNames != null)
            // This is a benign race
            return;

        String[] cols = new String[this.columns.size()];
        this.cachedKinds = new ContentsKind[this.columns.size()];
        int index = 0;
        for (Map.Entry<String, ColumnDescription> c: this.columns.entrySet()) {
            cols[index] = c.getKey();
            this.cachedKinds[index] = c.getValue().kind;
            index++;
        }
        // Important: this assignment must be made last
        this.cachedColumnNames = cols;
    }

    public String[] getColumnNames() {
        if (this.cachedColumnNames == null)
            this.seal();
        return this.cachedColumnNames;
    }

    public ContentsKind[] getColumnKinds() {
         if (this.cachedColumnNames == null)
             this.seal();
         return Converters.checkNull(this.cachedKinds);
    }

    public boolean containsColumnName(String columnName) {
        return columns.containsKey(columnName);
    }

    /**
     * Check whether this column name already exists.  If it does, change it to be
     * unique.
     * @param columnName  Column name that we plan to add to the schema.
     * @return            A column name based on this one which is unique.
     */
    public String generateColumnName(String columnName) {
        if (!this.containsColumnName(columnName))
            return columnName;
        int counter = 0;
        while (true) {
            String newName = columnName + " (" + counter + ")";
            if (!this.containsColumnName(newName))
                return newName;
            counter++;
        }
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
        StringBuilder result = new StringBuilder();
        String separator = "";
        for (ColumnDescription c: this.columns.values()) {
            result.append(separator).append(c.toString());
            separator = ", ";
        }
        return result.toString();
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

    public static Schema readFromJsonFile(Path file) {
        try {
            String s = new String(Files.readAllBytes(file));
            return Schema.fromJson(s);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void writeToJsonFile(Path file) {
        try {
            String text = this.toJson();
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public IAppendableColumn[] createAppendableColumns() {
        IAppendableColumn[] cols = new IAppendableColumn[this.getColumnCount()];
        int index = 0;
        for (ColumnDescription cd: this.columns.values()) {
            BaseListColumn col = BaseListColumn.create(cd);
            cols[index++] = col;
        }
        return cols;
    }
}

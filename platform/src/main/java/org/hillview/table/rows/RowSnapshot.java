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

package org.hillview.table.rows;

import com.google.gson.JsonElement;
import org.hillview.dataset.api.IJson;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * The copy of the data in a row of the table.
 * This is quite inefficient, it should be used rarely.
 */
public class RowSnapshot extends BaseRowSnapshot
        implements Serializable, IJson  {
    /**
     * Maps a column name to a value.
     */
    private final LinkedHashMap<String, Object> fields =
            new LinkedHashMap<String, Object>();
    private final int cachedHashcode;
    private final Schema schema;

    public RowSnapshot(final ITable data, final int rowIndex, final Schema schema) {
        List<IColumn> columns = data.getColumns(schema);
        this.schema = schema;
        for (IColumn c: columns) {
            if (c.isMissing(rowIndex)) {
                this.fields.put(c.getName(), null);
                continue;
            }
            ContentsKind kind = c.getKind();
            if (kind == ContentsKind.Date || kind == ContentsKind.Duration)
                this.fields.put(c.getName(), c.getDouble(rowIndex));
            else
                this.fields.put(c.getName(), c.getObject(rowIndex));
        }
        this.cachedHashcode = this.computeHashCode(schema);
    }

    /**
     * Creates a row snapshot taking the data from the specified table.
     * @param data     Table storing the data.
     * @param rowIndex Index of the row containing the data.
     */
    public RowSnapshot(final ITable data, final int rowIndex) {
        this(data, rowIndex, data.getSchema());
    }

    /**
     * Creates a row snapshot using a specified set of values.
     * @param schema  Schema; describes the columns in the row snapshot.
     * @param data    One value for each column in the schema.
     */
    private RowSnapshot(final Schema schema, final Object[] data) {
        if (schema.getColumnCount() != data.length)
            throw new RuntimeException("Mismatched schema");
        int index = 0;
        this.schema = schema;
        for (String col: schema.getColumnNames()) {
            this.fields.put(col, data[index]);
            index++;
        }
        this.cachedHashcode = this.computeHashCode(schema);
    }

    @Override
    public boolean exists() { return true; }

    public boolean isMissing(String colName) { return (this.fields.get(colName) == null); }

    @Override
    public int columnCount() {
        return this.fields.size();
    }

    @Override
    public List<String> getColumnNames() {
        return this.schema.getColumnNames();
    }

    @Override
    public Object getObject(String colName) {
        Object o = this.fields.get(colName);
        if (o == null)
            return null;
        ContentsKind kind = this.schema.getKind(colName);
        if (kind == ContentsKind.Date)
            return Converters.toDate(this.getDouble(colName));
        else if (kind == ContentsKind.Duration)
            return Converters.toDuration(this.getDouble(colName));
        return o;
    }

    public String getString(String colName) {
        return (String) this.fields.get(colName);
    }

    public String asString(String colName) {
        Object obj = this.getObject(colName);
        assert obj != null;
        return obj.toString();
    }

    public int getInt(String colName) {
        return (int)this.fields.get(colName);
    }

    public double getDouble(String colName) {
        return (double)this.fields.get(colName);
    }

    public Instant getDate(String colName) {
        if (this.isMissing(colName))
            return null;
        return Converters.toDate(this.getDouble(colName));
    }

    public Duration getDuration(String colName) {
        if (this.isMissing(colName))
            return null;
        return Converters.toDuration(this.getDouble(colName));
    }

    /**
     * When row snapshots are serialized as JSON some data types have to be converted.
     * @param data    Data to fill the row
     * @param schema  Row schema
     * @return        A row parsed from an array of objects deserialized from JSON.
     */
    public static RowSnapshot parse(Schema schema, Object[] data) {
        Object[] converted = new Object[data.length];
        List<String> cols = new ArrayList<String>(data.length);
        cols.addAll(schema.getColumnNames());
        for (int i = 0; i < data.length; i++) {
            String c = cols.get(i);
            ColumnDescription cd = schema.getDescription(c);
            Object o = data[i];
            if (o == null) {
                converted[i] = null;
            } else if (cd.kind == ContentsKind.Integer) {
                // In JSON everything is a double
                converted[i] = (int)(double)o;
            } else {
                // These should be doubles or strings.
                // No conversion needed.
                converted[i] = o;
            }
        }
        return new RowSnapshot(schema, converted);
    }

    @Override
    public JsonElement toJsonTree() {
        Object[] data = new Object[this.fields.size()];
        int index = 0;
        for (Object o : this.fields.values())
            data[index++] = o;
        return IJson.gsonInstance.toJsonTree(data);
    }

    // The following are Map interface methods.

    @Override
    public int size() {
        return this.fields.size();
    }

    @Override
    public boolean isEmpty() {
        return this.fields.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.fields.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        return this.fields.get(key);
    }

    @Override
    public Set<String> keySet() {
        return this.fields.keySet();
    }

    @Override
    public Collection<Object> values() {
        return this.fields.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return this.fields.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        RowSnapshot that = (RowSnapshot) o;
        return this.fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return this.cachedHashcode;
    }
}

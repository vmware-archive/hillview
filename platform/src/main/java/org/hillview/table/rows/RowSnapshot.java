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
import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.api.Interval;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.*;

/**
 * The copy of the data in a row of the table.
 * This is quite inefficient, it should be used rarely.
 * When the data is a date or duration it is represented instead by its
 * double encoding.
 */
public class RowSnapshot extends BaseRowSnapshot
        implements IJson, IJsonSketchResult {
    static final long serialVersionUID = 1;
    /**
     * Maps a column name to a value.
     */
    private final LinkedHashMap<String, Object> fields =
            new LinkedHashMap<String, Object>();
    private final long cachedHashcode;
    private final Schema schema;

    public RowSnapshot(final ITable data, final int rowIndex, final Schema schema) {
        List<IColumn> columns = data.getColumns(schema);
        this.schema = schema;
        for (IColumn c: columns) {
            if (c.isMissing(rowIndex)) {
                this.fields.put(c.getName(), null);
                continue;
            }
            this.fields.put(c.getName(), c.getData(rowIndex));
        }
        this.cachedHashcode = this.computeHashCode(schema);
    }

    /**
     * Create a row snapshot by projecting another one onto a specified schema.
     * @param other    Row snapshot to project.
     * @param schema   Schema to keep.
     */
    public RowSnapshot(RowSnapshot other, Schema schema) {
        this.schema = schema;
        for (ColumnDescription cd : schema.getColumnDescriptions())
            this.fields.put(cd.name, other.fields.get(cd.name));
        this.cachedHashcode = this.computeHashCode(this.schema);
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
    public RowSnapshot(final Schema schema, final Object[] data) {
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

    @Override
    public boolean isMissing(String colName) { return (this.fields.get(colName) == null); }

    @Override
    public double getEndpoint(String colName, boolean start) {
        Interval i = (Interval)(this.fields.get(colName));
        return i.get(start);
    }

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
        switch (this.schema.getKind(colName)) {
            case Date:
                return Converters.toDate((double)o);
            case Duration:
                return Converters.toDuration((double)o);
            case Time:
                return Converters.toTime((double)o);
            case LocalDate:
                return Converters.toLocalDate((double)o);
            case None:
            case String:
            case Json:
            case Double:
            case Integer:
            case Interval:
            default:
                return o;
        }
    }

    @Override
    @Nullable
    public String getString(String colName) {
        return (String) this.fields.get(colName);
    }

    @Override
    public String asString(String colName) {
        Object obj = this.getObject(colName);
        assert obj != null;
        return obj.toString();
    }

    @Override
    public int getInt(String colName) {
        return (int)this.fields.get(colName);
    }

    @Override
    public double getDouble(String colName) {
        return (double)this.fields.get(colName);
    }

    @Override
    public double asDouble(String colName) {
        return this.getDouble(colName);
    }

    /**
     * When row snapshots are serialized as JSON some data types have to be converted.
     * @param data    Data to fill the row
     * @param schema  Row schema
     * @param columnsMinimumValue  List of columns that may not have a value specified.
     *                        For these the minimum value will be used.
     * @return        A row parsed from an array of objects deserialized from JSON.
     */
    public static RowSnapshot parseJson(Schema schema,
                                        Object[] data,
                                        @Nullable String[] columnsMinimumValue) {
        HashSet<String> set = null;
        if (columnsMinimumValue != null)
            set = new HashSet<String>(Arrays.asList(columnsMinimumValue));
        Object[] converted = new Object[data.length];
        List<String> cols = new ArrayList<String>(data.length);
        cols.addAll(schema.getColumnNames());
        for (int i = 0; i < data.length; i++) {
            String c = cols.get(i);
            ColumnDescription cd = schema.getDescription(c);
            Object o = data[i];
            if (o == null) {
                if (set != null && set.contains(c)) {
                    ContentsKind kind = cd.kind;
                    converted[i] = kind.defaultValue();
                } else {
                    converted[i] = null;
                }
            } else if (cd.kind == ContentsKind.Integer) {
                // In JSON everything is a double
                converted[i] = Converters.toInt((double)o);
            } else if (cd.kind == ContentsKind.Interval) {
                @SuppressWarnings("unchecked")
                List<Double> arr = (List<Double>)o;
                converted[i] = new Interval(arr.get(0), arr.get(1));
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
        return Converters.foldHash(this.cachedHashcode);
    }
}

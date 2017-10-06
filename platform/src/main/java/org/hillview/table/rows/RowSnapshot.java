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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * The copy of the data in a row of the table.
 * This is quite inefficient, it should be used rarely.
 */
public class RowSnapshot extends BaseRowSnapshot implements Serializable, IJson {
    /**
     * Maps a column name to a value.
     */
    private final LinkedHashMap<String, Object> fields = new LinkedHashMap<String, Object>();
    private String[] fieldNames = new String[0];

    public RowSnapshot(final ITable data, final int rowIndex, final Schema schema) {
        IColumn[] columns = data.getColumns(schema);
        for (IColumn c: columns)
            this.fields.put(c.getName(), c.getObject(rowIndex));
        this.fieldNames = this.fields.keySet().toArray(new String[0]);
    }

    public RowSnapshot(final ITable data, final int rowIndex) {
        this(data, rowIndex, data.getSchema());
    }

    private RowSnapshot(final Schema schema, final Object[] data) {
        if (schema.getColumnCount() != data.length)
            throw new RuntimeException("Mismatched schema");
        int index = 0;
        for (String col: schema.getColumnNames())
            this.fields.put(col, data[index++]);
        this.fieldNames = this.fields.keySet().toArray(new String[0]);
    }

    public boolean isMissing(String colName) { return (this.fields.get(colName) == null); }

    @Override
    public int columnCount() {
        return this.fields.size();
    }

    @Override
    public String[] getColumnNames() {
        return this.fieldNames;
    }

    @Override
    public Object getObject(String colName) {
        return this.fields.get(colName);
    }

    public String getString(String colName) {
        return (String) this.fields.get(colName);
    }

    public int getInt(String colName) {
        return (int)this.fields.get(colName);
    }

    public double getDouble(String colName) {
        return (double)this.fields.get(colName);
    }

    public Instant getDate(String colName) {
        return (Instant)this.fields.get(colName);
    }

    public Duration getDuration( String colName) {
        return (Duration) this.fields.get(colName);
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
        cols.addAll(Arrays.asList(schema.getColumnNames()));
        for (int i = 0; i < data.length; i++) {
            String c = cols.get(i);
            ColumnDescription cd = schema.getDescription(c);
            Object o = data[i];
            if (o == null) {
                converted[i] = null;
            } else if (cd.kind == ContentsKind.Date) {
                converted[i] = Converters.toDate((double)o);
            } else if (cd.kind == ContentsKind.Integer) {
                // In JSON everything is a double
                converted[i] = (int)(double)o;
            } else {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        RowSnapshot that = (RowSnapshot) o;
        return this.fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return this.fields.hashCode();
    }
}

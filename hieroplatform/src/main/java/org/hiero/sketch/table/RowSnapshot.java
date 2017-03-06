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

package org.hiero.sketch.table;

import com.google.gson.JsonElement;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.IRow;
import org.hiero.sketch.table.api.ISubSchema;
import org.hiero.sketch.table.api.ITable;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;

/**
 * The copy of the data in a row of the table.
 * This is quite inefficient, it should be used rarely.
 */
public class RowSnapshot implements IRow, Serializable, IJson {
    protected final Schema schema;
    /**
     * Maps a column name to a value.
     */
    private final HashMap<String, Object> field = new HashMap<String, Object>();

    public RowSnapshot(final ITable data, final int rowIndex) {
        this.schema = data.getSchema();
        for (final String colName : this.schema.getColumnNames())
            this.field.put(colName, data.getColumn(colName).getObject(rowIndex));
    }

    public RowSnapshot(final ITable data, final int rowIndex, final ISubSchema subSchema) {
        this.schema = data.getSchema().project(subSchema);
        for (final String colName : this.schema.getColumnNames())
            this.field.put(colName, data.getColumn(colName).getObject(rowIndex));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        RowSnapshot that = (RowSnapshot) o;
        return this.schema.equals(that.schema) && this.field.equals(that.field);
    }

    @Override
    public int hashCode() {
        int result = this.schema.hashCode();
        result = (31 * result) + this.field.hashCode();
        return result;
    }

    @Override
    public int rowSize() {
        return this.field.size();
    }

    public boolean isMissing(String colName) { return (this.field.get(colName) == null); }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public Object get(String colName) {
        return this.field.get(colName);
    }

    public String getString(String colName) {
        return (String) this.field.get(colName);
    }

    public Integer getInt(String colName) {
        return (Integer) this.field.get(colName);
    }

    public Double getDouble( String colName) {
        return (Double) this.field.get(colName);
    }

    public LocalDateTime getDate( String colName) {
        return (LocalDateTime) this.field.get(colName);
    }

    public Duration getDuration( String colName) {
        return (Duration) this.field.get(colName);
    }

    private Object[] getData() {
        Object[] data = new Object[this.schema.getColumnCount()];
        int i = 0;
        for (final String nextCol: this.schema.getColumnNames())
            data[i++] = this.get(nextCol);
        return data;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Object o : this.getData()) {
            if (!first)
                builder.append(", ");
            builder.append(o.toString());
            first = false;
        }
        return builder.toString();
    }

    @Override
    public JsonElement toJsonTree() {
        Object[] data = this.getData();
        return IJson.gsonInstance.toJsonTree(data);
    }
}

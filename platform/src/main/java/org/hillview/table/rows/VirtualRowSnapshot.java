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

import org.hillview.table.Schema;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.*;

/**
 * A pointer to a (projection of a) row in a table. The projection is
 * specified by the schema.  This class is mutable; it is designed so that
 * it is allocated once, and then the row is set multiple times, probably
 * while iterating over a MembershipSet.  It allows accessing the row
 * without actually materializing the data in the row.
 * This class is NOT serializable and it is not thread-safe; it should only
 * be used as a local thread variable.
 *
 * VirtualRowSnapshot vrs = new VirtualRowSnapshot(table);
 * IRowIterator it = table.getMembershipSet().getIterator();
 * int row = it.getNextRow();
 * while (row >= 0) {
 *     vrs.setRow(row);  // point to next row
 *     Object val = vrs.get("Col");
 *     ...
 *     row = it.getNextRow();
 * }
 */
public class VirtualRowSnapshot extends BaseRowSnapshot implements ISketchWorkspace {
    static final long serialVersionUID = 1;

    /**
     * Table where the virtual row resides.
     */
    private final ITable table;
    /**
     * Index of the row in the table.
     * Note that this can be -1 if there are no rows in the table.
     */
    private int rowIndex = -1;
    private final Schema schema;
    protected final HashMap<String, IColumn> columns;

    public VirtualRowSnapshot(final ITable table, final Schema schema,
            @Nullable final Map<String, String> columnRenameMap) {
        this.table = table;
        this.schema = schema;
        this.columns = new HashMap<String, IColumn>();
        List<IColumn> cols = table.getLoadedColumns(schema.getColumnNames());
        for (IColumn col: cols) {
            String nameToUse = col.getName();
            if (columnRenameMap != null && columnRenameMap.containsKey(nameToUse))
                nameToUse = columnRenameMap.get(nameToUse);
            this.columns.put(nameToUse, col);
        }
    }

    public VirtualRowSnapshot(
            final ITable table,
            final Schema schema) {
        this(table, schema, null);
    }

    public VirtualRowSnapshot(final ITable table) {
        this(table, table.getSchema(), null);
    }

    public boolean exists() { return this.rowIndex >= 0; }

    public void setRow(final int rowIndex) {
        if (rowIndex < 0)
            throw new RuntimeException("Negative row index " + rowIndex);
        this.rowIndex = rowIndex;
    }

    public Schema getSchema() { return this.schema; }

    /**
     * Returns a real row snapshot corresponding to this virtual row snapshot.
     * If there is no such row returns null.
     */
    @Nullable
    public RowSnapshot materialize() {
        if (!this.exists())
            return null;
        return new RowSnapshot(this.table, this.rowIndex, this.schema);
    }

    @Override
    public int hashCode() {
        return Converters.foldHash(this.computeHashCode(this.schema));
    }

    @Override
    public int size() {
        return this.columns.size();
    }

    @Override
    public boolean isEmpty() {
        return this.columns.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.columns.containsKey(key);
    }

   @Override
    public Set<String> keySet() {
        return this.columns.keySet();
    }

    @Override
    public Collection<Object> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseRowSnapshot)) return false;
        return compareForEquality((BaseRowSnapshot)o, this.schema);
    }

    @Nullable
    protected IColumn getColumn(String colName) {
        return this.columns.get(colName);
    }

    public boolean isMissing(String colName) {
        if (!this.exists())
            throw new RuntimeException("No such row.");
        return this.getColumnChecked(colName).isMissing(this.rowIndex);
    }

    @Override
    public double getEndpoint(String colName, boolean start) {
        return this.columns.get(colName).getEndpoint(rowIndex, start);
    }

    @Override
    public int columnCount() {
        return this.schema.getColumnCount();
    }

    @Override
    public List<String> getColumnNames() {
        return this.schema.getColumnNames();
    }

    IColumn getColumnChecked(String colName) {
        IColumn col = this.getColumn(colName);
        if (col == null)
            throw new RuntimeException("No column named " + colName);
        return col;
    }

    @Override
    public Object getObject(String colName) {
        return this.getColumnChecked(colName).getObject(this.rowIndex);
    }

    @Override
    public String getString(String colName) {
        return this.getColumnChecked(colName).getString(this.rowIndex);
    }

    @Override
    public String asString(String colName) {
        return this.getColumnChecked(colName).asString(this.rowIndex);
    }

    @Override
    public int getInt(String colName) {
        return this.getColumnChecked(colName).getInt(this.rowIndex);
    }

    @Override
    public double getDouble(String colName) {
        return this.getColumnChecked(colName).getDouble(this.rowIndex);
    }

    @Override
    public Instant getDate(String colName) {
        return Converters.toDate(this.getColumnChecked(colName).getDouble(this.rowIndex));
    }

    @Override
    public LocalTime getTime(String colName) {
        return this.getColumnChecked(colName).getTime(this.rowIndex);
    }

    @Override
    public Duration getDuration(String colName) {
        return this.getColumnChecked(colName).getDuration(this.rowIndex);
    }

    @Override
    public double asDouble(String colName) {
        return this.getColumnChecked(colName).asDouble(this.rowIndex);
    }
}

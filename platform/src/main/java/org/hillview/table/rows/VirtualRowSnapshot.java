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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

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
public class VirtualRowSnapshot extends BaseRowSnapshot {
    /**
     * Table where the virtual row resides.
     */
    private final ITable table;
    /**
     * Index of the row in the table.
     */
    protected int rowIndex = -1;
    private final Schema schema;
    private final HashMap<String, IColumn> columns;

    public VirtualRowSnapshot(final ITable table, final Schema schema) {
        this.table = table;
        this.schema = schema;
        this.columns = new HashMap<String, IColumn>();
        ColumnAndConverterDescription[] ccds = ColumnAndConverterDescription.create(
                schema.getColumnNames());
        ColumnAndConverter[] cols = table.getLoadedColumns(ccds);
        for (ColumnAndConverter col: cols) {
            Converters.checkNull(col);
            this.columns.put(col.getName(), Converters.checkNull(col.column));
        }
    }

    public VirtualRowSnapshot(final ITable table) {
        this(table, table.getSchema());
    }

    public void setRow(final int rowIndex) {
        this.rowIndex = rowIndex;
    }

    public Schema getSchema() { return this.schema; }

    public RowSnapshot materialize() {
        return new RowSnapshot(this.table, this.rowIndex, this.table.getSchema());
    }

    @Override
    public int hashCode() {
        return computeHashCode(this.schema);
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

    protected IColumn getColumn(String colName) {
        return this.columns.get(colName);
    }

    public boolean isMissing(String colName) {
        return this.getColumn(colName).isMissing(this.rowIndex);
    }

    @Override
    public int columnCount() {
        return this.schema.getColumnCount();
    }

    @Override
    public String[] getColumnNames() {
        return this.schema.getColumnNames();
    }

    @Override
    public Object getObject(String colName) {
        return this.getColumn(colName).getObject(this.rowIndex);
    }

    @Override
    public String getString(String colName) {
        return this.getColumn(colName).getString(this.rowIndex);
    }

    @Override
    public int getInt(String colName) {
        return this.getColumn(colName).getInt(this.rowIndex);
    }

    @Override
    public double getDouble(String colName) {
        return this.getColumn(colName).getDouble(this.rowIndex);
    }

    @Override
    public Instant getDate(String colName) {
        return this.getColumn(colName).getDate(this.rowIndex);
    }

    @Override
    public Duration getDuration(String colName) {
        return this.getColumn(colName).getDuration(this.rowIndex);
    }
}
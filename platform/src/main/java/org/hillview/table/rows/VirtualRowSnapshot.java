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
import org.hillview.table.api.ITable;
import org.hillview.utils.HashUtil;

import java.time.Duration;
import java.time.Instant;

/**
 * A pointer to a (projection of a) row in a table. The projection is
 * specified by the schema.  This class is mutable; it is designed so that
 * it is allocated once, and then the row is set multiple times, probably
 * while iterating over a MembershipSet.  It allows accessing the row
 * without actually materializing the data in the row.
 * This class is NOT serializable.
 */
public class VirtualRowSnapshot extends BaseRowSnapshot {
    /**
     * Table where the virtual row resides.
     */
    private final ITable table;
    /**
     * Index of the row in the table.
     */
    private int rowIndex = -1;
    private final Schema schema;

    public VirtualRowSnapshot(final ITable table) {
        this.table = table;
        this.schema = table.getSchema();
    }

    public VirtualRowSnapshot(final ITable table, final Schema schema) {
        this.table = table;
        this.schema = schema;
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
        int hashCode = 31;
        for (String cn : this.schema.getColumnNames()) {
            Object o = this.getObject(cn);
            if (o == null)
                continue;
            hashCode = HashUtil.murmurHash3(hashCode, o.hashCode());
        }
        return hashCode;
    }

    public boolean isMissing(String colName) {
        return (this.table.getColumn(colName).isMissing(this.rowIndex));
    }

    @Override
    public int columnCount() {
        return this.schema.getColumnCount();
    }

    @Override
    public Iterable<String> getColumnNames() {
        return this.schema.getColumnNames();
    }

    @Override
    public Object getObject(String colName) {
        return this.table.getColumn(colName).getObject(this.rowIndex);
    }

    @Override
    public String getString(String colName) {
        return this.table.getColumn(colName).getString(this.rowIndex);
    }

    @Override
    public int getInt(String colName) {
        return this.table.getColumn(colName).getInt(this.rowIndex);
    }

    @Override
    public double getDouble(String colName) {
        return this.table.getColumn(colName).getDouble(this.rowIndex);
    }

    @Override
    public Instant getDate(String colName) {
        return this.table.getColumn(colName).getDate(this.rowIndex);
    }

    @Override
    public Duration getDuration(String colName) {
        return this.table.getColumn(colName).getDuration(this.rowIndex);
    }
}
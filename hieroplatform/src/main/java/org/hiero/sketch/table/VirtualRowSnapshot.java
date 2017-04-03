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

import org.hiero.sketch.table.api.ITable;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * A pointer to a (projection of a) row in a table (Stored as the row index). The projection is
 * specified by the schema.
 */
public class VirtualRowSnapshot extends BaseRowSnapshot {
    protected final ITable table;
    protected final int rowIndex;

    public VirtualRowSnapshot(final ITable table, final int rowIndex) {
        super(table.getSchema());
        this.table = table;
        this.rowIndex = rowIndex;
    }

    public VirtualRowSnapshot(final ITable table, final int rowIndex, final Schema schema) {
        super(schema);
        this.table = table;
        this.rowIndex = rowIndex;
    }

    public RowSnapshot materialize() {
        return new RowSnapshot(this.table, this.rowIndex, this.schema);
    }

    @Override
    public int hashCode() {
        int result = this.table.hashCode();
        for (String s : this.getSchema().getColumnNames())
            if (this.getObject(s) != null)
                result = (31 * result) + this.getObject(s).hashCode();
        return result;
    }

    public boolean isMissing(String colName) {
        return (this.table.getColumn(colName).isMissing(this.rowIndex));
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
    public Integer getInt(String colName) {
        return this.table.getColumn(colName).getInt(this.rowIndex);
    }

    @Override
    public Double getDouble( String colName) {
        return this.table.getColumn(colName).getDouble(this.rowIndex);
    }

    @Override
    public LocalDateTime getDate( String colName) {
        return this.table.getColumn(colName).getDate(this.rowIndex);
    }

    @Override
    public Duration getDuration( String colName) {
        return this.table.getColumn(colName).getDuration(this.rowIndex);
    }
}
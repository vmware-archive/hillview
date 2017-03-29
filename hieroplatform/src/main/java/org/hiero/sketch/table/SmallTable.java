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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ITable;

import java.io.Serializable;

/**
 * A SmallTable is similar to a Table, but it is intended to be shipped over the network.
 * We expect all columns to be serializable.
 */
public class SmallTable
        extends BaseTable
        implements Serializable, IJson {
    protected final Schema schema;
    protected final int rowCount;

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Create a small table from a list of columns and a precomputed schema.
     * @param columns  List of columns.
     * @param schema   The schema of the result; it must match the list of columns.
     */
    protected SmallTable(final Iterable<IColumn> columns, final Schema schema) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns) {
            s.append(c.getDescription());
            if (!(c instanceof Serializable))
                throw new RuntimeException("Column for SmallTable is not serializable");
        }
        this.schema = schema;
        if (!schema.equals(s))
            throw new RuntimeException("Schemas do not match");
    }


    public SmallTable(final Iterable<IColumn> columns) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns) {
            s.append(c.getDescription());
            if (!(c instanceof Serializable))
                throw new RuntimeException("Column for SmallTable is not serializable");
        }
        this.schema = s;
    }

    public SmallTable(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.rowCount = 0;
    }

    @Override
    public ITable selectRowsFromFullTable(IMembershipSet set) {
        return this.compress(set);
    }

    @Override
    public ITable project(Schema schema) {
        Iterable<IColumn> cols = this.getColumns(schema);
        return new SmallTable(cols, schema);
    }

    @Override
    public IRowIterator getRowIterator() {
        return new FullMembership.FullMembershipIterator(this.rowCount);
    }

    @Override
    public IMembershipSet getMembershipSet() {
        return new FullMembership(this.rowCount);
    }

    @Override
    public int getNumOfRows() {
        return this.rowCount;
    }

    private RowSnapshot[] getRows() {
        RowSnapshot[] rows = new RowSnapshot[this.getNumOfRows()];
        for (int i = 0; i < this.getNumOfRows(); i++)
            rows[i] = new RowSnapshot(this, i);
        return rows;
    }

    @Override
    public JsonElement toJsonTree() {
        JsonObject result = new JsonObject();
        result.add("schema", this.schema.toJsonTree());
        result.addProperty("rowCount", this.rowCount);
        RowSnapshot[] rows = this.getRows();
        JsonArray jrows = new JsonArray();
        for (RowSnapshot rs : rows)
            jrows.add(rs.toJsonTree());
        result.add("rows", jrows);
        return result;
    }
}

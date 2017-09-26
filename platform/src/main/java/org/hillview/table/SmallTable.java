/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ObjectArrayColumn;
import org.hillview.table.rows.RowSnapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A SmallTable is similar to a Table, but it is intended to be shipped over the network.
 * We expect all columns to be serializable.
 */
public class SmallTable extends BaseTable implements Serializable, IJson {
    final Schema schema;
    private final int rowCount;

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    public SmallTable() {
        super(new Schema());
        this.schema = new Schema();
        this.rowCount = 0;
    }

    /**
     * Create a small table from a list of columns and a precomputed schema.
     * @param columns  List of columns.
     * @param schema   The schema of the result; it must match the list of columns.
     */
    SmallTable(final Iterable<IColumn> columns, final Schema schema) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = schema;
        if (!schema.equals(s))
            throw new RuntimeException("Schemas do not match");
    }

    public SmallTable(final Iterable<IColumn> columns) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
    }

    public SmallTable(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.rowCount = 0;
    }

    static List<IColumn> colsFromRows(Schema schema, List<RowSnapshot> rows) {
        List<IColumn> colList = new ArrayList<IColumn>();
        for (String cn : schema.getColumnNames()) {
            ObjectArrayColumn col = new ObjectArrayColumn(schema.getDescription(cn), rows.size());
            for (int i = 0; i < rows.size(); i++)
                col.set(i, rows.get(i).getObject(cn));
            colList.add(col);
        }
        return colList;
    }

    public SmallTable(Schema schema, List<RowSnapshot> rowList) {
        super(colsFromRows(schema, rowList));
        this.schema = schema;
        this.rowCount = rowList.size();
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
        JsonArray jRows = new JsonArray();
        for (RowSnapshot rs : rows)
            jRows.add(rs.toJsonTree());
        result.add("rows", jRows);
        return result;
    }
}

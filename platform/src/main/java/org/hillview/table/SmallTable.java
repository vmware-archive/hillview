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

package org.hillview.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.table.api.*;
import org.hillview.table.columns.ObjectArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A SmallTable is similar to a Table, but it is intended to be shipped over the network.
 * We expect all columns to be serializable.  This means that we should only use
 * ObjectArrayColumns (except in tests); for example, the String*Columns are not serializable.
 * Some tests use non-serializable small tables.
 */
public class SmallTable extends BaseTable implements IJsonSketchResult {
    static final long serialVersionUID = 1;
    final Schema schema;
    private final int rowCount;

    @Nullable
    @Override
    public String getSourceFile() {
        /* TODO: We could also keep track of files for some small tables,
          but it is not clear it's worth it. */
        return null;
    }

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
    <T extends IColumn> SmallTable(final List<T> columns, final Schema schema) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = schema;
        if (!schema.equals(s))
            throw new RuntimeException("Schemas do not match");
        this.check();
    }

    public <T extends IColumn> SmallTable(final List<T> columns) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.check();
    }

    public <T extends IColumn> SmallTable(final T[] columns) {
        this(Arrays.asList(columns));
    }

    public SmallTable(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.rowCount = 0;
        this.check();
    }

    public void check() {
        if (this.columns.size() != this.schema.getColumnCount())
            throw new RuntimeException("Invalid table");
    }

    private static List<IColumn> colsFromRows(Schema schema, List<RowSnapshot> rows) {
        List<IColumn> colList = new ArrayList<IColumn>();
        for (String cn : schema.getColumnNames()) {
            ObjectArrayColumn col = new ObjectArrayColumn(schema.getDescription(cn), rows.size());
            for (int i = 0; i < rows.size(); i++)
                col.set(i, rows.get(i).get(cn));
            colList.add(col);
        }
        return colList;
    }

    public SmallTable(Schema schema, List<RowSnapshot> rowList) {
        super(colsFromRows(schema, rowList));
        this.schema = schema;
        this.rowCount = rowList.size();
        this.check();
    }

    @Override
    public ITable selectRowsFromFullTable(IMembershipSet set) {
        return this.compress(set);
    }

    @Override
    public ITable project(Schema schema) {
        List<IColumn> cols = this.getColumns(schema);
        return new SmallTable(cols, schema);
    }

    @Override
    public List<IColumn> getLoadedColumns(List<String> columns) {
        return Linq.map(columns, this::getColumn);
    }

    @Override
    public <T extends IColumn> ITable replace(List<T> columns) {
        return new SmallTable(columns);
    }

    @Override
    public IRowIterator getRowIterator() {
        return new FullMembershipSet.FullMembershipIterator(this.rowCount);
    }

    @Override
    public IMembershipSet getMembershipSet() {
        return new FullMembershipSet(this.rowCount);
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

    /**
     * Returns a concatenation of this table with the given table. The schema of this table is used, and if the given
     * table is not empty, its schema should be the same as this's schema.
     *
     * @param that The other SmallTable. It is concatenated to this table. If nonempty, its schema has to be the same
     *            as this schema.
     * @return A new SmallTable that is the concatenation of this schema with that schema.
     */
    public SmallTable concatenate(SmallTable that) {
        List<RowSnapshot> rows = new ArrayList<RowSnapshot>(this.getNumOfRows() + that.getNumOfRows());
        for (int i = 0; i < this.getNumOfRows(); i++)
            rows.add(new RowSnapshot(this, i));
        for (int i = 0; i < that.getNumOfRows(); i++)
            rows.add(new RowSnapshot(that, i));

        if (this.getSchema().getColumnCount() > 0) {
            return new SmallTable(this.getSchema(), rows);
        } else {
            return new SmallTable();
        }
    }
}

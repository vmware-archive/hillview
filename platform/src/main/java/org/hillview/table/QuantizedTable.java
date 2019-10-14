/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import org.hillview.table.api.*;
import org.hillview.table.columns.QuantizedColumn;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A quantized table wraps a table using a QuantizationSchema: each column
 * is quantized according to a different policy.
 */
public class QuantizedTable implements ITable {
    private final ITable table;
    private final QuantizationSchema quantizationSchema;

    public QuantizedTable(ITable table, QuantizationSchema quantizationSchema) {
        this.table = table;
        this.quantizationSchema = quantizationSchema;
    }

    @Nullable
    @Override
    public String getSourceFile() {
        return table.getSourceFile();
    }

    @Override
    public Schema getSchema() {
        return table.getSchema();
    }

    @Override
    public IRowIterator getRowIterator() {
        return table.getRowIterator();
    }

    @Override
    public IMembershipSet getMembershipSet() {
        return table.getMembershipSet();
    }

    @Override
    public int getNumOfRows() {
        return table.getNumOfRows();
    }

    @Override
    public SmallTable compress(String[] colNames, IRowOrder rowOrder) {
        IColumn[] compressedCols =
                Linq.map(colNames, s -> this.getColumn(s).compress(rowOrder), IColumn.class);
        return new SmallTable(compressedCols);
    }

    @Override
    public SmallTable compress(Schema schema, IRowOrder rowOrder) {
        List<String> colNames = schema.getColumnNames();
        List<IColumn> compressedCols =
                Linq.map(colNames, s -> this.getColumn(s).compress(rowOrder));
        return new SmallTable(compressedCols, schema);
    }

    @Override
    public SmallTable compress(IRowOrder rowOrder) {
        return this.compress(this.getSchema(), rowOrder);
    }

    @Override
    public List<IColumn> getColumns(Schema schema) {
        return Linq.map(this.table.getColumns(schema),
                c -> new QuantizedColumn(c, this.quantizationSchema.get(c.getName())));
    }

    @Override
    public IColumn getColumn(String name) {
        return new QuantizedColumn(this.table.getColumn(name), this.quantizationSchema.get(name));
    }

    @Override
    public ITable selectRowsFromFullTable(IMembershipSet set) {
        return new QuantizedTable(this.table.selectRowsFromFullTable(set), this.quantizationSchema);
    }

    @Override
    public ITable project(Schema schema) {
        return new QuantizedTable(table.project(schema), this.quantizationSchema);
    }

    @Override
    public <T extends IColumn> ITable replace(List<T> columns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends IColumn> ITable append(List<T> columns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<IColumn> getLoadedColumns(List<String> columns) {
        return Linq.map(this.table.getLoadedColumns(columns),
                c -> new QuantizedColumn(c, this.quantizationSchema.get(c.getName())));
    }

    @Override
    public ITable insertColumn(IColumn column, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toLongString(int rowsToDisplay) {
        return "Private/" + this.table.toLongString(rowsToDisplay);
    }
}

/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview.storage;

import org.hillview.table.ColumnDescription;
import org.hillview.table.LazySchema;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.LazyColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import java.util.List;

/**
 * This is a special form of the CsvFileLoader, which only loads the first column of the
 * schema when invoked.
 * The other columns will be loaded later, as invoked explicitly.
 */
public class LazyCsvFileLoader extends TextFileLoader {
    private final LazySchema schema;
    CsvFileLoader loader;

    public LazyCsvFileLoader(String path, CsvFileLoader.Config configuration, LazySchema schema) {
        super(path);
        this.schema = schema;
        this.allowFewerColumns = configuration.allowFewerColumns;
        if (this.schema.isNull())
            throw new RuntimeException("Schema guessing not supported for lazy csv loading");
        Schema firstColumn = new Schema(Converters.checkNull(
                this.schema.getSchema()).getColumnDescriptions().subList(0, 1));
        this.loader = new CsvFileLoader(path, configuration, new LazySchema(firstColumn));
    }

    @Override
    public void prepareLoading() {
        this.loader.prepareLoading();
    }

    public ITable loadFragment(int maxRows, boolean skip) {
        ITable table = this.loader.loadFragment(maxRows, skip);
        int rowCount = table.getNumOfRows();
        Schema schema = this.schema.getSchema();
        List<ColumnDescription> desc = Converters.checkNull(schema).getColumnDescriptions();
        Table result = Table.createLazyTable(desc, rowCount, this.filename, new NoLoader());
        String firstColName = this.schema.getSchema().getColumnNames().get(0);
        IColumn col0 = table.getLoadedColumn(firstColName);
        LazyColumn fc = result.getColumn(firstColName).as(LazyColumn.class);
        Converters.checkNull(fc).setData(col0);
        return result;
    }

    @Override
    public void endLoading() {
        this.loader.endLoading();
    }
}

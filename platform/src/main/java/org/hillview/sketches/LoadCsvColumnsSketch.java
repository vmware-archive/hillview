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

package org.hillview.sketches;

import org.hillview.dataset.api.ControlMessage;
import org.hillview.dataset.api.TableSketch;
import org.hillview.storage.CsvFileLoader;
import org.hillview.table.LazySchema;
import org.hillview.table.Schema;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.LazyColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;

/**
 * This is an unorthodox sketch; it actually mutates the table it is operating on
 * by loading a few more columns into the table.
 */
public class LoadCsvColumnsSketch
        extends ControlMessage.StatusListMonoid
        implements TableSketch<ControlMessage.StatusList> {

    private final Schema schema;

    public LoadCsvColumnsSketch(Schema schema) {
        this.schema = schema;
    }

    @Nullable
    @Override
    public ControlMessage.StatusList create(@Nullable ITable data) {
        HillviewLogger.instance.info("Loading CSV columns for table",
                "Columns are {0}", this.schema.toString());
        Converters.checkNull(data);
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.hasHeaderRow = true;
        CsvFileLoader loader = new CsvFileLoader(
                // The data will be in the same source file (data.getSourceFile()) which was used
                // initially to load the table.
                Converters.checkNull(data.getSourceFile()), config, new LazySchema(this.schema));
        ITable loaded = loader.load();
        Converters.checkNull(loaded);
        for (String c: this.schema.getColumnNames()) {
            IColumn ld = loaded.getLoadedColumn(c);
            LazyColumn lc = data.getColumn(c).as(LazyColumn.class);
            Converters.checkNull(lc);
            if (lc.sizeInRows() != ld.sizeInRows())
                throw new RuntimeException("Loaded column has different size from original column:" +
                        " file=" + data.getSourceFile() +
                        " loaded=" + ld.toString() + " size=" + ld.sizeInRows() +
                        " original=" + lc.toString() + " size=" + lc.sizeInRows());
            Converters.checkNull(lc).setData(ld);
        }
        return new ControlMessage.StatusList(new ControlMessage.Status("OK"));
    }
}

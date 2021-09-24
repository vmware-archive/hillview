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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.sketches.DistinctStrings;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Is given a column name and a set of key names.
 * The data in the column is expanded into key-value pairs
 * and a new string column is created for each key that appears in
 * the list supplied.  The column names are generated from the
 * key names by prepending the prefix to each one.
 * If a key has no associated value a null is inserted.
 */
public class ExplodeKeyValueColumnMap implements IMap<ITable, ITable> {
    static final long serialVersionUID = 1;
    private final String column;
    private final String prefix;
    private final DistinctStrings keys;

    public ExplodeKeyValueColumnMap(String column, String prefix, DistinctStrings keys) {
        this.column = column;
        this.prefix = prefix;
        this.keys = keys;
    }

    @Nullable
    @Override
    public ITable apply(@Nullable ITable data) {
        IColumn source = Converters.checkNull(data).getLoadedColumn(this.column);
        IMembershipSet set = data.getMembershipSet();
        ContentsKind kind = source.getKind();
        // Create columns for the result
        int colCount = this.keys.size();
        Map<String, IMutableColumn> columns = new HashMap<String, IMutableColumn>(colCount);
        for (String s: this.keys.getStrings()) {
            ColumnDescription cd = new ColumnDescription(this.prefix + s, kind);
            IMutableColumn col = BaseColumn.create(cd, set.getMax(), set.getSize());
            columns.put(s, col);
        }

        // Scan the source data and fill the columns.
        IRowIterator it = set.getIterator();
        int r = it.getNextRow();
        while (r >= 0) {
            String value = source.getString(r);
            final int row = r;
            for (String k: this.keys.getStrings())
                columns.get(k).setMissing(row);
            if (kind == ContentsKind.Json) {
                Utilities.forAllJsonFields(value, (c, v) -> {
                    IMutableColumn col = columns.get(this.prefix + c);
                    if (col == null)
                        throw new RuntimeException("Could not locate column " + this.prefix + c);
                    col.set(row, v.toString());
                });
            } else if (kind == ContentsKind.String) {
                Utilities.forAllKVPairs(Utilities.cleanupKVString(value), (c, v) -> {
                    IMutableColumn col = columns.get(this.prefix + c);
                    if (col == null)
                        throw new RuntimeException("Could not locate column " + this.prefix + c);
                    col.set(row, v);
                });
            } else {
                throw new RuntimeException("Unexpected column type: " + kind);
            }
            r = it.getNextRow();
        }
        List<IColumn> complete = data.getColumns(data.getSchema());
        for (String k: this.keys.getStrings()) {
            IColumn col = columns.get(k).seal();
            complete.add(col);
        }
        return data.replace(complete);
    }
}

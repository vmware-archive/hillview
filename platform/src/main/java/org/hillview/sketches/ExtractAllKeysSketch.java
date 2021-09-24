/*
 * Copyright (c) 2021 VMware Inc. All Rights Reserved.
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

import org.hillview.dataset.api.TableSketch;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;

/**
 * This is a sketch that treats a column as a key-value list of pairs.
 * It extracts all keys that show up anywhere at the top level.
 * E.g., in a JSON column that contains JSON objects this will return all JSON labels.
 * This sketch is not scalable: the number of distinct keys could potentially
 * larger than the number of rows.  Ideally this should do some kind of reservoir
 * sampling over the keys and only keep the frequent ones if the set is too large.
 */
public class ExtractAllKeysSketch implements TableSketch<DistinctStrings> {
    final String column;

    public ExtractAllKeysSketch(String column) {
        this.column = column;
    }

    @Nullable
    @Override
    public DistinctStrings create(@Nullable ITable data) {
        DistinctStrings result = new DistinctStrings();
        IColumn col = Converters.checkNull(data).getLoadedColumn(this.column);
        IRowIterator it = data.getMembershipSet().getIterator();
        int r = it.getNextRow();
        while (r >= 0) {
            String source = col.asString(r);
            if (col.getKind() == ContentsKind.Json) {
                @Nullable
                Iterable<String> strings = Utilities.getAllJsonFieldNames(source);
                if (strings != null)
                    result.addAll(strings);
            } else {
                Utilities.getAllKeys(Utilities.cleanupKVString(source), result::add);
            }
            r = it.getNextRow();
        }
        return result;
    }

    @Nullable
    @Override
    public DistinctStrings zero() {
        return new DistinctStrings();
    }

    @Nullable
    @Override
    public DistinctStrings add(@Nullable DistinctStrings left, @Nullable DistinctStrings right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}

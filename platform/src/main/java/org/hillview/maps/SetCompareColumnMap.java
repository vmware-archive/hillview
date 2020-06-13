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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.SetComparisonColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.Linq;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Given a set of tables, creates a new table with a new column
 * that is the comparison of the membership sets of all tables.
 * The first table is not used in the comparison, but the column is
 * appended to it.
 */
public class SetCompareColumnMap implements IMap<List<ITable>, ITable> {
    private final String columnName;
    private final List<String> names;

    public SetCompareColumnMap(String columnName, List<String> names) {
        this.columnName = columnName;
        this.names = names;
    }

    @Nullable
    @Override
    public ITable apply(@Nullable List<ITable> data) {
        Converters.checkNull(data);
        assert !data.isEmpty();
        ITable first = data.get(0);
        data = Utilities.tail(data);
        if (this.names.size() != data.size())
            throw new RuntimeException("Incompatible names and tables sizes: " +
                    names.size() + " and " + data.size());
        if (this.names.isEmpty())
            throw new RuntimeException("Empty names");
        String[] names = Utilities.toArray(this.names);
        IMembershipSet[] sets = Utilities.toArray(Linq.map(data, ITable::getMembershipSet), IMembershipSet.class);
        SetComparisonColumn col = new SetComparisonColumn(this.columnName, sets, names);
        return first.append(Collections.singletonList(col));
    }
}

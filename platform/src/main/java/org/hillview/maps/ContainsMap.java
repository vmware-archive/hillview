/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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
import org.hillview.table.Schema;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.membership.EmptyMembershipSet;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * This is a peculiar map.  It is applied to a table and checks to see
 * whether a specific (partial) row appears in the table.  If it does
 * the *whole* table, including all rows.  I.e., the map ignores the
 * membership map of the original table.  However, if the row is not
 * present, this returns an empty table.
 */
public class ContainsMap implements IMap<ITable, ITable> {
    private final Schema schema;
    private final RowSnapshot row;

    public ContainsMap(Schema schema, RowSnapshot row) {
        this.schema = schema;
        this.row = row;
    }

    @Nullable
    @Override
    public ITable apply(@Nullable ITable data) {
        VirtualRowSnapshot vw = new VirtualRowSnapshot(Converters.checkNull(data), this.schema);

        IMembershipSet set = new EmptyMembershipSet(data.getMembershipSet().getMax());
        IRowIterator rowIt = data.getRowIterator();
        for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
            vw.setRow(i);
            if (this.row.compareForEquality(vw, this.schema)) {
                set = new FullMembershipSet(data.getMembershipSet().getMax());
                break;
            }
        }
        return data.selectRowsFromFullTable(set);
    }
}

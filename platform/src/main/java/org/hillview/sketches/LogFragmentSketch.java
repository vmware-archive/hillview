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

package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.membership.BlockRowOrder;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;

/**
 * This sketch is applicable to Tables that represent log files,
 * and thus have a specific schema.  It returns a number of rows
 * from the table.
 */
public class LogFragmentSketch implements ISketch<ITable, NextKList> {
    private final Schema schema;
    @Nullable
    private final RowSnapshot row;
    @Nullable
    private final Schema rowSchema;
    private final int count;
    /**
     * If start is -1 then the row indicates the location of the data.
     * We will bring a few rows above this row (if they exist).
     * Otherwise start is used as the row number.
     */
    private final int start;

    public LogFragmentSketch(Schema schema, @Nullable RowSnapshot row,
                             @Nullable Schema rowSchema, int start, int count) {
        this.schema = schema;
        this.row = row;
        this.rowSchema = rowSchema;
        this.count = count;
        this.start = start;
    }

    @Nullable
    @Override
    public NextKList create(@Nullable ITable data) {
        assert data != null;
        int index = 0;
        if (this.start >= 0) {
            index = this.start;
        } else {
            if (this.row != null) {
                IRowIterator it = data.getMembershipSet().getIterator();
                index = it.getNextRow();
                assert this.rowSchema != null;
                VirtualRowSnapshot vrs = new VirtualRowSnapshot(data, this.rowSchema);
                while (index >= 0) {
                    vrs.setRow(index);
                    if (row.compareForEquality(vrs, this.rowSchema)) {
                        // Bring rows around this index.
                        index = Math.max(0, index - 10);
                        break;
                    }
                    index = it.getNextRow();
                }
            }
        }
        if (index < 0)
            // Empty list.
            return new NextKList(data.getSchema());
        int maxCount = Math.min(this.count, data.getNumOfRows() - index);
        if (maxCount < 0)
            maxCount = 0;
        BlockRowOrder bro = new BlockRowOrder(index, maxCount);
        SmallTable rows = data.compress(this.schema, bro);
        IntList ones = new IntArrayList();
        for (int i = index; i < index + maxCount; i++)
            ones.add(1);
        return new NextKList(rows, ones, (long)index, (long)data.getNumOfRows());
    }

    @Nullable
    @Override
    public NextKList zero() {
        return new NextKList(this.schema);
    }

    @Nullable
    @Override
    public NextKList add(@Nullable NextKList left, @Nullable NextKList right) {
        assert left != null;
        assert right != null;
        if (left.isEmpty())
            return right;
        if (right.isEmpty())
            return left;
        throw new RuntimeException("Did not expect to merge two non-empty log fragments");
    }
}

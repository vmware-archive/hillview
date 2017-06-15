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
 *
 */

package org.hillview.table;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.set.strategy.mutable.UnifiedSetWithHashingStrategy;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Implements a set interface for BaseRowSnapshot;
 * we are using custom equality and hashcode functions.
 */
public class RowSnapshotSet implements Serializable {
    private final Schema schema;
    private final UnifiedSetWithHashingStrategy<BaseRowSnapshot> rowSet;

    private RowSnapshotSet(Schema schema) {
        this.schema = schema;
        HashingStrategy<BaseRowSnapshot> strategy = new HashingStrategy<BaseRowSnapshot>() {
            @Override
            public int computeHashCode(BaseRowSnapshot row) {
                return row.computeHashCode(RowSnapshotSet.this.schema);
            }

            @Override
            public boolean equals(BaseRowSnapshot left, BaseRowSnapshot right) {
                return left.compareForEquality(right, RowSnapshotSet.this.schema);
            }
        };
        this.rowSet = new UnifiedSetWithHashingStrategy<BaseRowSnapshot>(strategy);
    }

    public <T extends BaseRowSnapshot> RowSnapshotSet(Schema schema, Iterable<T> data) {
        this(schema);
        for (BaseRowSnapshot row : data)
            this.add(row);
    }

    private void add(final BaseRowSnapshot row) {
        this.rowSet.add(row);
    }

    public boolean contains(final BaseRowSnapshot row) {
        return this.rowSet.contains(row);
    }

    public void forEach(Consumer<BaseRowSnapshot> action) {
        this.rowSet.forEach(action);
    }

    static class SetTableFilter implements TableFilter, Serializable {
        private final RowSnapshotSet set;
        @Nullable
        private VirtualRowSnapshot vrs;
        @Nullable
        private ITable table;

        SetTableFilter(RowSnapshotSet set) {
            this.set = set;
            this.table = null;
            this.vrs = null;
        }

        @Override
        public void setTable(ITable table) {
            this.table = table;
            this.vrs = new VirtualRowSnapshot(table);
        }

        @Override
        public boolean test(int rowIndex) {
            Converters.checkNull(this.vrs).setRow(rowIndex);
            return this.set.contains(this.vrs);
        }
    }

    public TableFilter rowInTable() {
        return new SetTableFilter(this);
    }
}

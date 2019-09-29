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

package org.hillview.table.rows;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.hillview.table.Schema;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilterDescription;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Implements a set interface for BaseRowSnapshot;
 * we are using custom equality and hashcode functions.
 */
public class RowSnapshotSet implements Serializable {
    private final Schema schema;
    private final ObjectSet<BaseRowSnapshot> rowSet;

    private RowSnapshotSet(Schema schema) {
        this.schema = schema;
        this.rowSet = new ObjectOpenCustomHashSet<BaseRowSnapshot>(
                          new BaseRowSnapshotHashingStrategy(schema));
    }

    public <T extends BaseRowSnapshot> RowSnapshotSet(Schema schema, Iterable<T> data) {
        this(schema);
        for (BaseRowSnapshot row : data)
            this.add(row);
    }

    private void add(final BaseRowSnapshot row) {
        this.rowSet.add(row);
    }

    private boolean contains(final BaseRowSnapshot row) {
        return this.rowSet.contains(row);
    }

    public void forEach(Consumer<BaseRowSnapshot> action) {
        this.rowSet.forEach(action);
    }

    static class SpecializedTableFilter implements ITableFilter {
        private final RowSnapshotSet set;
        private final VirtualRowSnapshot vrs;
        private boolean includeSet;

        SpecializedTableFilter(RowSnapshotSet set, boolean includeSet, ITable table) {
            this.set = set;
            this.includeSet = includeSet;
            this.vrs = new VirtualRowSnapshot(table, this.set.schema);
        }

        @Override
        public boolean test(int rowIndex) {
            this.vrs.setRow(rowIndex);
            return includeSet == this.set.contains(this.vrs);
        }
    }

    public static class SetTableFilterDescription implements ITableFilterDescription {
        private final RowSnapshotSet set;
        private final boolean includeSet;

        public SetTableFilterDescription(RowSnapshotSet set, boolean includeSet) {
            this.set = set;
            this.includeSet = includeSet;
        }

        @Override
        public ITableFilter getFilter(ITable table) {
            return new SpecializedTableFilter(this.set, this.includeSet, table);
        }
    }

    static class BaseRowSnapshotHashingStrategy implements
            Hash.Strategy<BaseRowSnapshot>, Serializable {
        private final Schema schema;

        BaseRowSnapshotHashingStrategy(final Schema schema) {
            this.schema = schema;
        }

        @Override
        public int hashCode(BaseRowSnapshot row) {
            return row.computeHashCode(this.schema);
        }

        @Override
        public boolean equals(BaseRowSnapshot left, @Nullable BaseRowSnapshot right) {
            if (right == null)
                return left == null;
            return left.compareForEquality(right, this.schema);
        }
    }

    /* public ITableFilterDescription rowInTable() {
        return new SetTableFilterDescription(this);
    }
    */
}

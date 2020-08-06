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

package org.hillview.table.columns;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;

import javax.annotation.Nullable;

/**
 * An empty column does not really store any data - all values are null.
 */
public class EmptyColumn extends BaseColumn implements IAppendableColumn, IMutableColumn {
    static final long serialVersionUID = 1;

    private int size;
    private boolean sealed;

    public EmptyColumn(String name, int size) {
        super(new ColumnDescription(name, ContentsKind.None));
        this.size = size;
        this.sealed = true;
    }

    public EmptyColumn(ColumnDescription desc) {
        super(desc);
        this.size = 0;
        this.sealed = false;
    }

    @Override
    public IColumn seal() {
        this.sealed = true;
        return this;
    }

    @Override
    public void setMissing(int rowIndex) { }

    @Override
    public void appendMissing() {
        if (this.sealed)
            throw new RuntimeException("Appending to sealed column");
        this.size++;
    }

    @Override
    public boolean isMissing(int rowIndex) {
        return true;
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        if (s == null || s.isEmpty())
            this.appendMissing();
        else
            throw new RuntimeException("Appending value `" + s + "` to empty column " +
                    this.getName() + " row " + this.sizeInRows());
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    @Override
    public double asDouble(int rowIndex) {
        throw new RuntimeException("asDouble of value in null column");
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        return null;
    }

    @Override
    public IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final int i, final int j) {
                return 0;
            }
        };
    }

    @Override
    public IColumn rename(String newName) {
        return new EmptyColumn(newName, this.size);
    }

    @Override
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet unused) {
        return new EmptyColumn(new ColumnDescription(newColName, kind));
    }

    @Override
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        return 0;
    }
}

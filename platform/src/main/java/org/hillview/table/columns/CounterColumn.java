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

package org.hillview.table.columns;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IIntColumn;

/**
 * A CounterColumn has constant data: row i contains value i + offset.
 */
public class CounterColumn extends BaseColumn implements IIntColumn {
    static final long serialVersionUID = 1;

    private final int size;
    private final int offset;

    public CounterColumn(final String name, int size, int offset) {
        super(new ColumnDescription(name, ContentsKind.Integer));
        this.size = size;
        this.offset = offset;
    }

    public CounterColumn(final String name, int size) {
        this(name, size, 0);
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
    public int getInt(int rowIndex) {
        return this.offset + rowIndex;
    }

    @Override
    public IColumn rename(String newName) {
        return new CounterColumn(newName, this.size, this.offset);
    }

    @Override
    public boolean isMissing(int rowIndex) {
        return false;
    }
}

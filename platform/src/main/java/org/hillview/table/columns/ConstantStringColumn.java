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
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IStringColumn;

import javax.annotation.Nullable;

/**
 * A ConstantStringColumn is a column where all values are identical.
 */
public class ConstantStringColumn extends BaseColumn implements IStringColumn {
    public final int size;
    @Nullable
    private final String value;

    public ConstantStringColumn(final ColumnDescription desc, int size, @Nullable String value) {
        super(desc);
        this.size = size;
        this.value = value;
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
    public String getString(int rowIndex) {
        return this.value;
    }

    @Override
    public IColumn rename(String newName) {
        return new ConstantStringColumn(
                this.description.rename(newName),
                this.size, this.value);
    }

    @Override
    public boolean isMissing(int rowIndex) {
        return this.value == null;
    }
}

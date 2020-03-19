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

package org.hillview.table.columns;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.IMutableColumn;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Adds a missing bit vector to BaseColumn.
 */
public abstract class BaseArrayColumn extends BaseColumn {
    static final long serialVersionUID = 1;
    @Nullable
    private final BitSet missing;

    BaseArrayColumn(final ColumnDescription description, final int size) {
        super(description);
        if (size < 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        if (!description.kind.isObject())
            this.missing = new BitSet(size);
        else
            this.missing = null;
    }

    @Override
    public boolean isLoaded() { return true; }

    @Override
    public boolean isMissing(final int rowIndex) {
        assert this.missing != null;
        return this.missing.get(rowIndex);
    }

    public void setMissing(final int rowIndex) {
        assert this.missing != null;
        this.missing.set(rowIndex);
    }

    /**
     * Create an empty column with the specified description.
     * @param description Column description.
     */
    public static IMutableColumn create(ColumnDescription description, int size) {
        switch (description.kind) {
            case Json:
            case String:
                return new StringArrayColumn(description, size);
            case Date:
                return new DateArrayColumn(description, size);
            case Integer:
                return new IntArrayColumn(description, size);
            case Double:
                return new DoubleArrayColumn(description, size);
            case Duration:
                return new DurationArrayColumn(description, size);
            default:
                throw new RuntimeException("Unexpected column kind " + description.toString());
        }
    }
}

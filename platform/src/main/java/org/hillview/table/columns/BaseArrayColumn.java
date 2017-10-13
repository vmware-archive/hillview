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
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Adds a missing bit vector to BaseColumn (if missing values are allowed)
 */
public abstract class BaseArrayColumn extends BaseColumn implements Serializable {
    protected BitSet missing;

    public BaseArrayColumn(final ColumnDescription description, final int size) {
        super(description);
        if (size < 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        this.missing = new BitSet(size);
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        // Ignore the analysis complaint. this.missing can not be null.
        return this.missing.get(rowIndex);
    }

    public void setMissing(final int rowIndex) {
        // Ignore the analysis complaint. this.missing can not be null.
        this.missing.set(rowIndex);
    }

    /**
     * Create an empty column with the specified description.
     * @param description Column description.
     */
    public static BaseArrayColumn create(ColumnDescription description) {
        switch (description.kind) {
            case Category:
            case Json:
            case String:
                return new StringArrayColumn(description, 0);
            case Date:
                return new DateArrayColumn(description, 0);
            case Integer:
                return new IntArrayColumn(description, 0);
            case Double:
                return new DoubleArrayColumn(description, 0);
            case Duration:
                return new DurationArrayColumn(description, 0);
            default:
                throw new RuntimeException("Unexpected column kind " + description.toString());
        }
    }
}

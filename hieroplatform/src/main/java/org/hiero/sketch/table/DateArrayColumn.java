/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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

package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDateColumn;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.time.LocalDateTime;

/*
 * Column of dates, implemented as an array of dates and a BitSet of missing values
 */
public final class DateArrayColumn
        extends BaseArrayColumn
        implements IDateColumn {
    private final LocalDateTime[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Date)
            throw new InvalidParameterException("Kind should be Date" + this.description.kind);
    }

    public DateArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new LocalDateTime[size];
    }

    public DateArrayColumn(final ColumnDescription description,
                           final LocalDateTime[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        return this.data[rowIndex];
    }

    private void set(final int rowIndex, @Nullable final LocalDateTime value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.getDate(rowIndex) == null; }

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}

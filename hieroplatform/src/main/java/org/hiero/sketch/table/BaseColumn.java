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

import org.hiero.sketch.table.api.IColumn;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Base class for all columns.
 */
abstract class BaseColumn implements IColumn {

    final ColumnDescription description;

    BaseColumn( final ColumnDescription description) {
        this.description = description;
    }


    @Override
    public ColumnDescription getDescription() {
        return this.description;
    }

    @Override
    public double getDouble(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMissing(final int rowIndex) { throw new UnsupportedOperationException(); }
}

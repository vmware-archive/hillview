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

package org.hillview.table.api;

import javax.annotation.Nullable;

/**
 * Interface implemented by a column whose data can be mutated.
 * Columns are mutable only while being read, afterwards this interface is
 * never used.
 */
public interface IMutableColumn extends IColumn {
    @SuppressWarnings("UnusedReturnValue")
    IColumn seal();

    default void set(final int rowIndex, @Nullable final String value) {
        throw new UnsupportedOperationException();
    }
    default void set(final int rowIndex, final int value) {
        throw new UnsupportedOperationException();
    }
    default void set(final int rowIndex, final double value) {
        throw new UnsupportedOperationException();
    }
    void setMissing(final int rowIndex);
}

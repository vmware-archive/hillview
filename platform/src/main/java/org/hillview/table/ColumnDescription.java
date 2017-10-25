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

package org.hillview.table;

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.ContentsKind;

import java.io.Serializable;

/**
 * Describes the contents of a column in a local table.
 */
public class ColumnDescription implements Serializable, IJson {
    public final String name;
    public final ContentsKind kind;
    /**
     * If true the column can have missing values (called NULL in databases).
     */
    public final boolean allowMissing;

    public ColumnDescription() {
        this.name = "";
        this.kind = ContentsKind.Category;
        this.allowMissing = false;
    }

    public ColumnDescription(final String name, final ContentsKind kind,
                             final boolean allowMissing) {
        this.name = name;
        this.kind = kind;
        this.allowMissing = allowMissing;
        if (name.isEmpty())
            throw new RuntimeException("Column names cannot be empty");
    }

    public void validate() {
        // This can happen because column descriptions may be read from external files.
        if (this.name == null || this.kind == null)
            throw new RuntimeException("null field in column description");
    }

    @Override public String toString() {
        return this.name + "(" + this.kind.toString() + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        final ColumnDescription that = (ColumnDescription) o;

        if (this.allowMissing != that.allowMissing) return false;
        return this.name.equals(that.name) && (this.kind == that.kind);
    }

    @Override
    public int hashCode() {
        int result = this.name.hashCode();
        result = (31 * result) + this.kind.hashCode();
        result = (31 * result) + (this.allowMissing ? 1 : 0);
        return result;
    }
}

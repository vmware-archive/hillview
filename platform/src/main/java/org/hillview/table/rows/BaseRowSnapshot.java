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

import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IRow;
import org.hillview.table.api.IStringFilter;
import org.hillview.utils.HashUtil;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An abstract class that implements IRow, which is an interface for accessing rows in a table.
 * Concrete classes that extend it are RowSnapshot and VirtualRowSnapshot. The main methods this
 * class provides are equality testing and comparison (by some specified record order). This for
 * instance allows easy comparison between the classes mentioned above.
 */
public abstract class BaseRowSnapshot implements IRow, Serializable {
    /**
     * Compare this row to the other for equality.
     * Only the fields in the schema are compared.
     */
    @SuppressWarnings("ConstantConditions")
    public boolean compareForEquality(BaseRowSnapshot other, Schema schema) {
        if (!this.exists())
            return false;

        List<String> columns = schema.getColumnNames();
        List<ContentsKind> kinds = schema.getColumnKinds();
        for (int i = 0; i < columns.size(); i++) {
            String cn = columns.get(i);
            boolean thisMissing = this.isMissing(cn);
            boolean otherMissing = other.isMissing(cn);
            if (thisMissing && otherMissing)
                continue;
            if (thisMissing || otherMissing)
                return false;
            boolean same;
            switch (kinds.get(i)) {
                case String:
                case Json:
                    same = this.asString(cn).equals(other.asString(cn));
                    break;
                case Integer:
                    same = this.getInt(cn) == other.getInt(cn);
                    break;
                case Date:
                case Double:
                case Duration:
                    same = this.getDouble(cn) == other.getDouble(cn);
                    break;
                default:
                    throw new RuntimeException("Unexpected kind " + schema.getKind(cn));
            }
            if (!same)
                return false;
        }
        return true;
    }

    public int computeHashCode(Schema schema) {
        if (!this.exists())
            return 0;
        int hashCode = 31;
        List<String> columns = schema.getColumnNames();
        List<ContentsKind> kinds = schema.getColumnKinds();
        for (int i = 0; i < columns.size(); i++) {
            String cn = columns.get(i);
            if (this.isMissing(cn))
                continue;
            switch (kinds.get(i)) {
                case String:
                case Json:
                    //noinspection ConstantConditions
                    hashCode = HashUtil.murmurHash3(hashCode, this.asString(cn).hashCode());
                    break;
                case Integer:
                    hashCode = HashUtil.murmurHash3(hashCode, this.getInt(cn));
                    break;
                case Date:
                case Double:
                case Duration:
                    hashCode = HashUtil.murmurHash3(hashCode, Double.hashCode(this.getDouble(cn)));
                    break;
                default:
                    throw new RuntimeException("Unexpected kind " + schema.getKind(cn));
            }
        }
        return hashCode;
    }

    /**
     * Return true if the string representation of any of the fields in this
     * row matches the specified filter.
     */
    public boolean matches(IStringFilter filter) {
        if (!this.exists())
            return false;
        List<String> columns = this.getColumnNames();
        for (String column : columns) {
            String field = this.asString(column);
            if (filter.test(field))
                return true;
        }
        return false;
    }

    /**
     * Compare this row to the other for ordering.
     * Only the fields in the schema are compared.
     */
    @SuppressWarnings("ConstantConditions")
    public int compareTo(BaseRowSnapshot other, RecordOrder ro) {
        if (!this.exists() || !other.exists())
            throw new RuntimeException("Comparing non-existing row.");
        for (int i = 0; i < ro.getSize(); i++) {
            ColumnSortOrientation cso = ro.getOrientation(i);
            String cn = cso.columnDescription.name;
            boolean thisMissing = this.isMissing(cn);
            boolean otherMissing = other.isMissing(cn);
            int c;
            if (thisMissing && otherMissing)
                c = 0;
            else if (thisMissing)
                c = 1;
            else if (otherMissing)
                c = -1;
            else switch (cso.columnDescription.kind) {
                case String:
                case Json:
                    c = this.asString(cn).compareTo(other.asString(cn));
                    break;
                case Integer:
                    c = Integer.compare(this.getInt(cn), other.getInt(cn));
                    break;
                case Date:
                case Double:
                case Duration:
                    c = Double.compare(this.getDouble(cn), other.getDouble(cn));
                    break;
                default:
                    throw new RuntimeException("Unexpected kind " + cso.columnDescription.kind);
            }
            if (!cso.isAscending)
                c = -c;
            if (c != 0)
                return c;
        }
        return 0;
    }

    @Override
    public String toString() {
        if (!this.exists())
            return "<no such row>";
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String cn : this.getColumnNames()) {
            Object o = this.getObject(cn);
            if (!first)
                builder.append(",");
            if (o != null)
                builder.append(o.toString());
            else
                builder.append("NULL");
            first = false;
        }
        return builder.toString();
    }

    @Override
    @Nullable
    public Object get(Object key) {
        return this.getObject((String)key);
    }

    // Unsupported Map operations

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Search by value is not supported.");
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException("Rows are read-only.");
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException("Rows are read-only.");
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        throw new UnsupportedOperationException("Rows are read-only.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Rows are read-only.");
    }
}

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

import org.hillview.sketches.ColumnSortOrientation;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IRow;
import org.hillview.utils.HashUtil;

import java.io.Serializable;

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
        String[] columns = schema.getColumnNames();
        ContentsKind[] kinds = schema.getColumnKinds();
        for (int i = 0; i < columns.length; i++) {
            String cn = columns[i];
            boolean thisMissing = this.isMissing(cn);
            boolean otherMissing = other.isMissing(cn);
            if (thisMissing && otherMissing)
                continue;
            if (thisMissing || otherMissing)
                return false;
            boolean same;
            switch (kinds[i]) {
                case Category:
                case String:
                case Json:
                    same = this.getString(cn).equals(other.getString(cn));
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
        int hashCode = 31;
        for (String cn: schema.getColumnNames()) {
            if (this.isMissing(cn))
                continue;
            switch (schema.getKind(cn)) {
                case Category:
                case String:
                case Json:
                    //noinspection ConstantConditions
                    hashCode = HashUtil.murmurHash3(hashCode, this.getString(cn).hashCode());
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
     * Compare this row to the other for ordering.
     * Only the fields in the schema are compared.
     */
    @SuppressWarnings("ConstantConditions")
    public int compareTo(BaseRowSnapshot other, RecordOrder ro) {
        for (ColumnSortOrientation cso: ro) {
            String cn = cso.columnDescription.name;
            int c;
            if (this.isMissing(cn) && other.isMissing(cn))
                c = 0;
            else if (this.isMissing(cn))
                c = 1;
            else if (other.isMissing(cn))
                c = -1;
            else switch (cso.columnDescription.kind) {
                case Category:
                case String:
                case Json:
                    c = this.getString(cn).compareTo(other.getString(cn));
                    break;
                case Date:
                    c = this.getDate(cn).compareTo(other.getDate(cn));
                    break;
                case Integer:
                    c = Integer.compare(this.getInt(cn), other.getInt(cn));
                    break;
                case Double:
                    c = Double.compare(this.getDouble(cn), other.getDouble(cn));
                    break;
                case Duration:
                    c = this.getDuration(cn).compareTo(other.getDuration(cn));
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
}
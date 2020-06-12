/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IStringColumn;

/**
 * This column combines data from several MembershipSets of the same size.
 * It has values indicating for each row which of the membership sets contain
 * the specific row.  For example, consider membership sets A[0,0,1,1] and B[0,1,1,0].
 * Then this column will contain the values [None,B,All,A].
 */
public class SetComparisonColumn extends BaseColumn implements IStringColumn {
    static final long serialVersionUID = 1;

    final IMembershipSet[] sets;
    final String[] labels;
    private final String[] values;

    public SetComparisonColumn(String name, IMembershipSet[] sets, String[] labels) {
        super(new ColumnDescription(name, ContentsKind.String));
        this.sets = sets;
        this.labels = labels;
        if (sets.length == 0)
            throw new RuntimeException("No sets to compare");
        if (sets.length != labels.length)
            throw new RuntimeException("Sets " + this.sets.length + " do not correspond to labels " + labels.length);
        if (this.sets.length > 8)
            throw new RuntimeException("Too many sets in colun " + this.sets.length);
        int values = 1 << this.sets.length;
        this.values = new String[values];
        this.values[0] = "None";
        this.values[values - 1] = "All";
        for (int j = 1; j < this.sets.length; j++) {
            if (sets[0].getMax() != sets[j].getMax())
                throw new RuntimeException("Incompatible sets compared");
        }
        for (int i = 1; i < values - 1; i++) {
            StringBuilder builder = new StringBuilder();
            int v = i;
            boolean first = true;
            for (int j = 0; j < this.sets.length; j++) {
                if ((v & 1) != 0) {
                    if (first) {
                        first = false;
                    } else {
                        builder.append(",");
                    }
                    builder.append(this.labels[j]);
                }
                v >>= 1;
            }
            this.values[i] = builder.toString();
        }
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public int sizeInRows() {
        return this.sets[0].getMax();
    }

    @Override
    public String getString(int rowIndex) {
        int index = 0;
        int mask = 1;
        for (IMembershipSet set : this.sets) {
            if (set.isMember(rowIndex))
                index |= mask;
            mask <<= 1;
        }
        return this.values[index];
    }

    @Override
    public IColumn rename(String newName) {
        return new SetComparisonColumn(newName, this.sets, this.labels);
    }

    @Override
    public boolean isMissing(int rowIndex) {
        return false;
    }
}

/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.sketches;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.util.TreeSet;

/**
 * A class that would hold a the set of distinct strings from a column bounded in size by maxSize.
 * If maxSize == 0 it holds all distinct strings in the column.
 */
public class DistinctStrings implements IJson {
    private final int maxSize;
    private final TreeSet<String> uniqueStrings;
    private final boolean bounded;
    private boolean truncated;  // if true we are missing some data
    private long columnSize;

    public DistinctStrings(final int maxSize) {
        if (maxSize < 0)
            throw new IllegalArgumentException("size of DistinctString should be positive");
        this.maxSize = maxSize;
        this.bounded = maxSize != 0;
        this.uniqueStrings = new TreeSet<String>();
        this.columnSize = 0;
        this.truncated = false;
    }

    public void add(@Nullable String string) {
        if (this.truncated)
            return;
        if ((this.bounded) && (this.uniqueStrings.size() == this.maxSize)) {
            if (!this.uniqueStrings.contains(string))
                this.truncated = true;
            return;
        }
        this.uniqueStrings.add(string);
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public int size() { return this.uniqueStrings.size(); }

    /**
     * @return the union of two sets. The maxSize is the larger of the two. If one
     * of them allow for unbounded size (maxSize = 0) then so does the union.
     */
    public DistinctStrings union(final DistinctStrings otherSet) {
        DistinctStrings result = new DistinctStrings(Math.max(this.maxSize, otherSet.maxSize));
        result.columnSize = this.columnSize + otherSet.columnSize;
        result.truncated = this.truncated || otherSet.truncated;

        for (String item: this.uniqueStrings)
            result.add(item);
        for (String item : otherSet.uniqueStrings)
            result.add(item);
        return result;
    }

    public Iterable<String> getStrings() {
        return this.uniqueStrings;
    }
}

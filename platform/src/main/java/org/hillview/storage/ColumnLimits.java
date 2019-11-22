/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.storage;

import org.hillview.table.filters.RangeFilterDescription;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;

/**
 * Stores for each column a pair of bounds represented as a RangeFilter.
 * This is used for doing filtering in SQL databases without materializing views.
 */
public class ColumnLimits {
    // For each column the range allowed after filtering
    private final HashMap<String, RangeFilterDescription> columnLimits;

    public ColumnLimits() {
        this.columnLimits = new HashMap<String, RangeFilterDescription>();
    }

    public ColumnLimits(ColumnLimits other) {
        this.columnLimits = new HashMap<String, RangeFilterDescription>(other.columnLimits);
    }

    @Nullable
    public RangeFilterDescription get(String column) {
        return this.columnLimits.get(column);
    }

    public void put(RangeFilterDescription filter) {
        this.columnLimits.put(filter.cd.name, filter);
    }

    Collection<RangeFilterDescription> allFilters() {
        return this.columnLimits.values();
    }

    public void intersect(RangeFilterDescription filter) {
        RangeFilterDescription existing = this.get(filter.cd.name);
        if (existing == null)
            this.put(filter);
        else
            this.put(existing.intersect(filter));
    }
}

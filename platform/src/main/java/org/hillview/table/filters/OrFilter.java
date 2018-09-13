/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.table.filters;

import org.hillview.table.api.ITableFilter;

/**
 * This filter executes two other filters and returns true when either returns true.
 */
public class OrFilter implements ITableFilter {
    private final ITableFilter first;
    private final ITableFilter second;

    OrFilter(ITableFilter first, ITableFilter second) {
        this.first = first;
        this.second = second;
    }

    public boolean test(int rowIndex) {
        return this.first.test(rowIndex) || this.second.test(rowIndex);
    }

    public String toString() {
        return "FilterOr(" + this.first.toString() + " || " + this.second.toString() + ")";
    }
}

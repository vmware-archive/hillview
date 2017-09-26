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

import org.hillview.table.api.IndexComparator;

import java.util.List;

public class ListComparator extends IndexComparator {
    private final List<IndexComparator> comparatorList;

    public ListComparator(final List<IndexComparator> comparatorList) {
        this.comparatorList = comparatorList;
    }

    @Override
    public int compare(final Integer o1, final Integer o2) {
        for (final IndexComparator aComparator : this.comparatorList) {
            final int val = aComparator.compare(o1, o2);
            if (val != 0) { return val; }
        }
        return 0;
    }
}
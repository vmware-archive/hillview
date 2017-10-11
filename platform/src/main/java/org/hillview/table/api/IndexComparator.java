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

import it.unimi.dsi.fastutil.ints.IntComparator;

/**
 * A comparator which compares two values given by their integer indexes in an array/column/table.
 * The default comparator sorts in ascending order, whereas the reverse comparator sorts in
 * descending order. Missing values are treated as + infinity and appear at the very end of the
 * ascending order. The default implementations are in IIntColumn etc.
 */
public abstract class IndexComparator implements IntComparator {
    /**
     * The reverse comparator.
     */
    public IndexComparator rev() {
        return new IndexComparator() {
            @Override
            public int compare(final int o1, final int o2) {
                return IndexComparator.this.compare(o2, o1);
            }
        };
    }
}

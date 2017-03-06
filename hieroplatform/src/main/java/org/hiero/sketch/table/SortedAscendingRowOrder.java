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
 *
 */

package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IRowOrder;
import org.hiero.sketch.table.api.IndexComparator;

import java.util.Arrays;

/**
 * Given a (table and a) IndexComparator, gives an iterator for the rows of the table in sorted order
 */
public class SortedAscendingRowOrder implements IRowOrder {
    private Integer[] order;

    /**
     * @param size Number of rows
     * @param indexComparator Defines the ordering
     */
    public SortedAscendingRowOrder(final int size, final IndexComparator indexComparator) {
        for (int i = 0; i < size; i++)
            this.order[i] = i;
        Arrays.sort(this.order, indexComparator);
    }

    @Override
    public int getSize() {
        return this.order.length;
    }

    @Override
    public IRowIterator getIterator() {
        return new IRowIterator() {
            private int current = 0;

            @Override
            public int getNextRow() {
                if (this.current < SortedAscendingRowOrder.this.order.length) {
                    final int i = SortedAscendingRowOrder.this.order[this.current];
                    this.current++;
                    return i;
                } else {
                    return -1;
                }
            }
        };
    }
}

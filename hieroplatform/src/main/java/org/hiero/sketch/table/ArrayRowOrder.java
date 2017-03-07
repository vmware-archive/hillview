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

import java.util.*;

/**
 * ArrayRowOrder takes an array, which is meant to represent a sequence of rows in a table.
 * The iterator returns those rows in sequence.
 */
public class ArrayRowOrder implements IRowOrder {
    private final List<Integer> sortedRows;
    private final int size;

    public ArrayRowOrder(final int[] order) {
        this.size = order.length;
        this.sortedRows = new ArrayList<Integer>(this.size);
        for (int i = 0; i < this.size; i++) {
            this.sortedRows.add(order[i]);
        }
    }

    public ArrayRowOrder(final Integer[] order) {
        this.size = order.length;
        this.sortedRows = new ArrayList<Integer>(this.size);
        this.sortedRows.addAll(Arrays.asList(order).subList(0, this.size));
    }

    public ArrayRowOrder(final Iterator<Integer> it) {
        int tmp = 0;
        this.sortedRows = new ArrayList<>();
        while (it.hasNext()) {
            this.sortedRows.add(it.next());
            tmp++;
        }
        this.size = tmp;
    }

    public ArrayRowOrder(final Iterable<Integer> iterable) {
        int tmp = 0;
        this.sortedRows = new ArrayList<>();
        for(Integer i : iterable) {
            this.sortedRows.add(i);
            tmp++;
        }
        this.size = tmp;
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public IRowIterator getIterator() {
        return new IRowIterator() {
            private int current = 0;

            @Override
            public int getNextRow() {
                if (this.current < ArrayRowOrder.this.size) {
                    this.current++;
                    return ArrayRowOrder.this.sortedRows.get(this.current - 1);
                } else {
                    return -1;
                }
            }
        };
    }
}
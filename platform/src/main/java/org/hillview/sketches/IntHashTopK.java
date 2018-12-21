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

package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.*;
import org.hillview.table.api.IndexComparator;
import org.hillview.table.rows.VirtualRowHashStrategy;
import org.hillview.utils.MutableInteger;

/**
 * Implementation of IntTopK that uses
 * 1) A hashMap for membership queries (should be O(1)).
 * 2) A priority queue to maintain the current largest element in the top K.
 * It does not seem to be much faster than IntTreeTopK, especially for small values of K.
 */

public class IntHashTopK implements IntTopK {
    private final int maxSize;
    private int size;
    private final Int2ObjectOpenCustomHashMap<MutableInteger> data;
    private int cutoff; /* max value that currently belongs to Top K. */
    private final IntComparator greater;
    private final IntHeapPriorityQueue queue;

    public IntHashTopK(final int maxSize, VirtualRowHashStrategy strategy, final IndexComparator greater) {
        this.maxSize = maxSize;
        this.size = 0;
        this.greater = greater;
        this.data = new Int2ObjectOpenCustomHashMap<MutableInteger>(strategy);
        this.queue = new IntHeapPriorityQueue(greater.rev());
    }

    public Int2IntSortedMap getTopK() {
        final Int2IntSortedMap finalMap = new Int2IntRBTreeMap(this.greater);
        this.data.forEach((k, v) -> finalMap.put(k.intValue(), v.get()));
        return finalMap;
    }

    public void push(final int intVal) {
        if (this.size == 0) {
            this.data.put(intVal, new MutableInteger(1)); // Add newVal to Top K
            this.cutoff = intVal;
            this.size = 1;
            return;
        }
        final int gt = this.greater.compare(intVal, this.cutoff);
        if (gt <= 0) {
            final MutableInteger counter = this.data.get(intVal);
            if (counter != null) { //Already in Top K, increase count. Size, cutoff do not change
                final int count = counter.get() + 1;
                counter.set(count);
            } else { // Add a new key to Top K
                this.data.put(intVal, new MutableInteger(1));
                this.queue.enqueue(intVal);
                if (this.size >= this.maxSize) {        // Remove the largest key, compute the new largest key
                    this.data.remove(this.cutoff);
                    this.cutoff = this.queue.dequeueInt();
                } else {
                    this.size += 1;
                }
            }
        } else {   // gt equals 1
            if (this.size < this.maxSize) {   // Only case where newVal needs to be added
                this.size += 1;
                this.data.put(intVal, new MutableInteger(1));     // Add newVal to Top K
                this.queue.enqueue(this.cutoff);
                this.cutoff = intVal;    // It is now the largest value
            }
        }
    }
}


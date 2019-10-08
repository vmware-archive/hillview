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

package org.hillview.sketches.results;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;

/**
 * A data structure that stores a set of row indices. These row indices represent a
 * sample of distinct elements from some (group of) columns.
 *
 * Currently this is done by storing those rows that hash to the minimum values. We store
 * the row indices of the k rows that hash to the minimum value (for some column to which the hash
 * is applied). We assume no collisions in the hash function. Finally, we will replace the row
 * indices by the values of the column at those indices. This structure is used to iterate over the
 * table (and avoid boxing/unboxing).
 */
public class MinKRows {
    /**
     * The parameter k in min k hash values.
     */
    public final int maxSize;
    /**
     * The actual number, which can be smaller if there are fewer distinct keys.
     */
    private int curSize;
    /**
     * The largest hash value currently in the min k.
     */
    private long cutoff;
    /**
     * A hashmap consisting of <hashValue, rowIndex> pairs.
     */
    public final Long2IntOpenHashMap hashMap;
    /**
     * A priority queue of negated hashValues (we use negations since it dequeues the smallest key).
     */
    LongHeapPriorityQueue priorityQueue;

    public MinKRows(final int maxSize) {
        this.maxSize = maxSize;
        this.hashMap = new Long2IntOpenHashMap(maxSize);
        this.priorityQueue = new LongHeapPriorityQueue(maxSize);
        this.curSize = 0;
        this.cutoff = 0;
    }

    public void push(long hashKey, int row) {
        if (this.curSize == 0) {
            this.hashMap.put(hashKey, row);
            this.priorityQueue.enqueue(-hashKey);
            this.cutoff = hashKey;
            this.curSize += 1;
        }
        if (!hashMap.containsKey(hashKey)) { // Does not have it already
            if (hashKey < this.cutoff) {  // It is below cutoff
                this.hashMap.put(hashKey, row);
                this.priorityQueue.enqueue(-hashKey);
                if (this.curSize == this.maxSize) {
                    this.hashMap.remove(this.cutoff);
                    this.priorityQueue.dequeueLong();
                    this.cutoff = -this.priorityQueue.firstLong();
                } else
                    this.curSize += 1;
            } else { // we ignore hash collisions.
                if (this.curSize < this.maxSize) {
                    this.curSize += 1;
                    this.hashMap.put(hashKey, row);
                    this.priorityQueue.enqueue(-hashKey);
                    this.cutoff = hashKey;
                }
            }
        }
    }
}

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

package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.Int2IntRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.hillview.utils.MutableInteger;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Implements an int specialized version of the ITopK interface. Reduces boxing
 * overheads in comparison to the TreeTopK class.
 */
public class IntTreeTopK implements ITopK<Integer> {
    private final int maxSize;
    private int size;
    private final Int2ObjectRBTreeMap<MutableInteger> data;
    private int cutoff; /* max value that currently belongs to Top K. */
    private final IntComparator greater;

    public IntTreeTopK(final int maxSize, final IntComparator greater) {
        this.maxSize = maxSize;
        this.size = 0;
        this.greater = greater;
        this.data = new Int2ObjectRBTreeMap<MutableInteger>(this.greater);
    }

    public Int2IntSortedMap getTopK() {
        final Int2IntSortedMap finalMap = new Int2IntRBTreeMap(this.greater);
        this.data.forEach((k, v) -> finalMap.put(k.intValue(), v.get()));
        return finalMap;
    }

    @Deprecated
    @Override
    public void push(final Integer newVal) {
        push(newVal.intValue());
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
                if (this.size >= this.maxSize) {        // Remove the largest key, compute the new largest key
                    this.data.remove(this.cutoff);
                    this.cutoff = this.data.lastIntKey();
                } else {
                    this.size += 1;
                }
            }
        } else {   // gt equals 1
            if (this.size < this.maxSize) {   // Only case where newVal needs to be added
                this.size += 1;
                this.data.put(intVal, new MutableInteger(1));     // Add newVal to Top K
                this.cutoff = intVal;    // It is now the largest value
            }
        }
    }
}

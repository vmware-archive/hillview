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

package org.hillview.sketches;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;
/**
 * Implements the ITopK interface as a SortedMap (which uses Red-Black trees).
 * Seems faster than HashMap implementation.
 */
public class TreeTopK<T> implements ITopK<T> {
    private final int maxSize;
    private int size;
    private final SortedMap<T, Integer> data;
    @Nullable
    private T cutoff; /* max value that currently belongs to Top K. */
    private final Comparator<T> greater;

    public TreeTopK(final int maxSize, final Comparator<T> greater) {
        this.maxSize = maxSize;
        this.size = 0;
        this.greater = greater;
        this.data = new TreeMap<T, Integer>(this.greater);
    }

    @Override
    public SortedMap<T, Integer> getTopK() {
        return this.data;
    }

    @Override
    public void push(final T newVal) {
        if (this.size == 0) {
            this.data.put(newVal, 1); // Add newVal to Top K
            this.cutoff = newVal;
            this.size = 1;
            return;
        }
        final int gt = this.greater.compare(newVal, this.cutoff);
        if (gt <= 0) {
            if (this.data.containsKey(newVal)) { //Already in Top K, increase count. Size, cutoff do not change
                final int count = this.data.get(newVal) + 1;
                this.data.put(newVal, count);
            } else { // Add a new key to Top K
                this.data.put(newVal, 1);
                if (this.size >= this.maxSize) {        // Remove the largest key, compute the new largest key
                    this.data.remove(this.cutoff);
                    this.cutoff = this.data.lastKey();
                } else {
                    this.size += 1;
                }
            }
        } else {   // gt equals 1
            if (this.size < this.maxSize) {   // Only case where newVal needs to be added
                this.size += 1;
                this.data.put(newVal, 1);     // Add newVal to Top K
                this.cutoff = newVal;    // It is now the largest value
            }
        }
    }
}

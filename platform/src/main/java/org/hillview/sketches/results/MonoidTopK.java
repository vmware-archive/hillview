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

import org.hillview.dataset.api.IMonoid;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This monoid is a sorted list of T objects, up to size K.
 * @param <T> Type of items in sorted list.
 */
public class MonoidTopK<T> implements IMonoid<SortedMap<T, Integer>> {
    private final int maxSize;
    private final Comparator<T> greater;

    /**
     *
     * @param maxSize the K in TopK, the size of the list.
     * @param greater The greaterThan comparator, we want the smallest elements in this order.
     */
    public MonoidTopK(final int maxSize, final Comparator<T> greater) {
        this.maxSize = maxSize;
        this.greater = greater;
    }

    /**
     * Zero is the empty list.
     */
    @Override
    public SortedMap<T, Integer> zero() {
        return new TreeMap<T, Integer>(this.greater);
    }

    /**
     * Addition is merge sort.
     */
    @Override @Nullable
    public SortedMap<T, Integer> add(
            @Nullable SortedMap<T, Integer> left,
            @Nullable SortedMap<T, Integer> right) {
        assert left != null;
        assert right != null;

        final Iterator<T> itLeft = left.keySet().iterator();
        final Iterator<T> itRight = right.keySet().iterator();
        T leftKey = (itLeft.hasNext()) ? itLeft.next() : null;
        T rightKey = (itRight.hasNext()) ? itRight.next() : null;
        final TreeMap<T, Integer> mergedMap = new TreeMap<T, Integer>(this.greater);

        while ((mergedMap.size() < this.maxSize) && ((leftKey != null) || (rightKey != null))) {
            if (leftKey == null) {
                mergedMap.put(rightKey, right.get(rightKey));
                rightKey = (itRight.hasNext()) ? itRight.next() : null;
            } else if (rightKey == null) {
                mergedMap.put(leftKey, left.get(leftKey));
                leftKey = (itLeft.hasNext()) ? itLeft.next() : null;
            } else {
                if (this.greater.compare(leftKey, rightKey) > 0) {
                    mergedMap.put(rightKey, right.get(rightKey));
                    rightKey = (itRight.hasNext()) ? itRight.next() : null;
                } else if (this.greater.compare(leftKey, rightKey) < 0) {
                    mergedMap.put(leftKey, left.get(leftKey));
                    leftKey = (itLeft.hasNext()) ? itLeft.next() : null;
                } else { //Keys are equal
                    mergedMap.put(leftKey, left.get(leftKey) + right.get(rightKey));
                    leftKey = (itLeft.hasNext()) ? itLeft.next() : null;
                    rightKey = (itRight.hasNext()) ? itRight.next() : null;
                }
            }
        }
        return mergedMap;
    }
}

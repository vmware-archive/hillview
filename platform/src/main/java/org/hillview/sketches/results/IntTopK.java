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
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;

/**
 * Interface for computing the topK elements of a data set, ordered by a comparator, with
 * counts for how often each of them occurs. This requires
 * - Membership: is it already present?
 * - Maximum: If not present, compare to the Maximum value currently in the Top K
 * - Insertion: for adding a new element.
 * We assume that all elements are positive.
 * In general the values we insert in this data structure are indexes of the actual values
 * in an array/column/list.
 */
public interface IntTopK {
    /**
     * @return a SortedMap of the top K elements, giving elements and their counts.
     */
    Int2IntSortedMap getTopK();
    /**
     * Tries to add a new value newVal to the data structure.
     * @param newVal value to add to the data structure.
     */
    void push(int newVal);
}

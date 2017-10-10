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

import java.util.SortedMap;

/**
 * Interface for computing the topK elements of a data set of type T, ordered by a comparator, with
 * counts for how often each of them occurs. getTopK returns a SortedMap of the top K elements,
 * giving elements and their counts.
 * push tries to add a new value newVal to the data structure. This requires
 * - Membership: is it already present?
 * - Maximum: If not present, compare to the Maximum value currently in the Top K
 * - Insert: If we need to Insert newVal
 */
interface ITopK<T> {
    SortedMap<T, Integer> getTopK();
    void push(T newVal);
}

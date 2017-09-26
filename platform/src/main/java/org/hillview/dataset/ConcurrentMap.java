/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview.dataset;

import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.Pair;

public class ConcurrentMap<T, S1, S2> implements IMap<T, Pair<S1, S2>> {
    private final IMap<T, S1> first;
    private final IMap<T, S2> second;

    public ConcurrentMap(IMap<T, S1> first, IMap<T, S2> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public Pair<S1, S2> apply(T data) {
        S1 first = this.first.apply(data);
        S2 second = this.second.apply(data);
        return new Pair<S1, S2>(first, second);
    }
}

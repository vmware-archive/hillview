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

package org.hiero.sketch.dataset.api;

import java.io.Serializable;

/**
 * A simple polymorphic pair with non-nullable components.
 * (How come Java does not have such a class built-in?)
 * Technically this class is serializable only if both T and S are, but there is no simple
 * way to check it statically, and we don't want a separate SerializablePair class.
 * (Same observation applies for the for IJson interface).
 * @param <T>  First element in the pair.
 * @param <S>  Second element in the pair.
 */
public class Pair<T, S> implements Serializable, IJson {
    public final T first;
    public final S second;

    public Pair(final T first, final S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;
        final Pair<?, ?> pair = (Pair<?, ?>) o;
        if (!this.first.equals(pair.first)) return false;
        return this.second.equals(pair.second);
    }

    @Override
    public int hashCode() {
        int result = this.first.hashCode();
        result = (31 * result) + this.second.hashCode();
        return result;
    }
}

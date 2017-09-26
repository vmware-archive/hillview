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

package org.hillview.dataset.api;

import javax.annotation.Nullable;
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
    @Nullable
    public final T first;
    @Nullable
    public final S second;

    public Pair(@Nullable final T first, @Nullable final S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if ((this.first != null) ? !this.first.equals(pair.first) : (pair.first != null)) return false;
        return (this.second != null) ? this.second.equals(pair.second) : (pair.second == null);
    }

    @Override
    public int hashCode() {
        int result = (this.first != null) ? this.first.hashCode() : 0;
        result = (31 * result) + ((this.second != null) ? this.second.hashCode() : 0);
        return result;
    }
}

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

package org.hillview.dataset.api;

/**
 * Similar to Pair, but components can never be null.
 * @param <T>  First element in the pair.
 * @param <S>  Second element in the pair.
 */
public class NonNullPair<T, S> implements IJson /* Serializable implied by IJson*/ {
    static final long serialVersionUID = 1;
    
    public final T first;
    public final S second;

    public NonNullPair(final T first, final S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        NonNullPair<?, ?> pair = (NonNullPair<?, ?>) o;

        if (!this.first.equals(pair.first)) return false;
        return this.second.equals(pair.second);
    }

    @Override
    public int hashCode() {
        int result = this.first.hashCode();
        result = 31 * result + this.second.hashCode();
        return result;
    }
}

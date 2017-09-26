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

public class Triple<T, S, V> implements Serializable, IJson {
    @Nullable
    public final T first;
    @Nullable
    public final S second;
    @Nullable
    public final V third;

    public Triple(@Nullable final T first, @Nullable final S second, @Nullable final V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;

        if ((this.first != null) ? !this.first.equals(triple.first) : (triple.first != null)) return false;
        if ((this.second != null) ? !this.second.equals(triple.second) : (triple.second != null)) return false;
        return (this.third != null) ? this.third.equals(triple.third) : (triple.third == null);
    }

    @Override
    public int hashCode() {
        int result = (this.first != null) ? this.first.hashCode() : 0;
        result = (31 * result) + ((this.second != null) ? this.second.hashCode() : 0);
        result = (31 * result) + ((this.third != null) ? this.third.hashCode() : 0);
        return result;
    }
}

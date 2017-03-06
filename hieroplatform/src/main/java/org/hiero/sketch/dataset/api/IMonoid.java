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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

/**
 * A monoid structure.
 * @param <R> Type of data representing an element of the monoid.
 *           R is not nullable; one should use Optional[R] if null is desired.
 */
public interface IMonoid<R> extends Serializable {
    @Nullable R zero();
    @Nullable R add(@Nullable R left, @Nullable R right);

    @Nullable
    default R reduce(List<R> data) {
        // This implementation avoids allocating a zero
        // if the list is non-empty.
        if (data.isEmpty())
            return this.zero();

        R result = data.get(0);
        // add the rest of the elements
        for (int i = 1; i < data.size(); i++)
            result = this.add(result, data.get(i));
        return result;
    }
}

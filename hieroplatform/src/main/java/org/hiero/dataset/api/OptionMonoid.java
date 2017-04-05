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

package org.hiero.dataset.api;

import javax.annotation.Nullable;

/**
 * A simple monoid with two elements: null and some fixed object of type T.
 * null is the neutral element.
 * @param <T> Type of value.
 */
public class OptionMonoid<T> implements IMonoid<T> {
    @Override @Nullable
    public T zero() { return null; }

    /**
     * Add two values.  If both values are not null, they are expected
     * to be the same value.  This is not checked, since it could be expensive.
     * @param left  Null or some value of type T.
     * @param right Null or some value of type T.
     * @return null if both are null, or else the non-null value.
     */
    @Override
    @Nullable public T add(@Nullable final T left, @Nullable final T right) {
        return (left != null) ? left : right;
    }
}

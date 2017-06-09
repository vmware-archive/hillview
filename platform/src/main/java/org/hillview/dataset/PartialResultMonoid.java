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

package org.hillview.dataset;

import org.hillview.dataset.api.IMonoid;
import org.hillview.dataset.api.PartialResult;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A Partial result with a value from a monoid is also a monoid.  This class implements
 * the induced monoid over PartialResult[T]
 * @param <T> Type of value from a monoid.
 */
public class PartialResultMonoid<T> implements IMonoid<PartialResult<T>> {
    /**
     * Monoid over values of type T.
     */
    private final IMonoid<T> monoid;

    public PartialResultMonoid( final IMonoid<T> monoid) {
        this.monoid = monoid;
    }

    @Nullable
    public PartialResult<T> zero() {
        return new PartialResult<T>(0.0, this.monoid.zero());
    }

    @Override @Nullable
    public PartialResult<T> add(@Nullable PartialResult<T> left,
                                @Nullable PartialResult<T> right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        return new PartialResult<T>(left.deltaDone + right.deltaDone,
                this.monoid.add(left.deltaValue, right.deltaValue));
    }
}

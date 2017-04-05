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
import org.hiero.utils.Converters;
import java.io.Serializable;

/**
 * Describes a sketch computation on a dataset of type T that produces a result of type R.
 * This class is also a monoid which knows how to combine two values of type R using the add
 * method.
 * @param <T> Input data type.
 * @param <R> Output data type.
 */
public interface ISketch<T, R> extends Serializable, IMonoid<R> {
    /**
     * Sketch computation on some dataset T.
     * @param data  Data to sketch.
     * @return  A sketch of the data.
     */
    R create(T data);

    /**
     * Helper method to return non-null zeros.
     */
    default R getZero() { return Converters.checkNull(this.zero()); }
}

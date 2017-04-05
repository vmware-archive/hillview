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

import java.io.Serializable;

/**
 * A closure that runs a computation on an object of type T
 * and returns an object of type S.
 * @param <T> Input type.
 * @param <S> Output type.
 */
public interface IMap<T, S> extends Serializable {
    /**
     * Apply a transformation to the data.
     * @param data Data to transform.
     * @return The result of the transformation.
     */
    S apply(T data);
}

/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;

import java.io.Serializable;

import javax.annotation.Nullable;

/**
 * A precomputed sketch looks like a sketch, but does not really run the 'create' method;
 * it just returns a precomputed value directly in all methods.
 * @param <T>  Type of the data sketched.
 * @param <R>  Type of result produced.
 */
public class PrecomputedSketch<T, R extends Serializable> implements ISketch<T, R> {
    static final long serialVersionUID = 1;
    
    private final R result;

    public PrecomputedSketch(R result) {
        this.result = result;
    }

    @Nullable
    @Override
    public R create(@Nullable T data) {
        return this.result;
    }

    @Nullable
    @Override
    public R zero() {
        return this.result;
    }

    @Nullable
    @Override
    public R add(@Nullable R left, @Nullable R right) {
        return this.result;
    }
}

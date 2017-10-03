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

package org.hillview.utils;

import org.hillview.dataset.api.IMonoid;

import javax.annotation.Nullable;

public class JsonListMonoid<T> implements IMonoid<JsonList<T>> {
    @Nullable
    @Override
    public JsonList<T> zero() {
        return new JsonList<T>();
    }

    @Nullable
    @Override
    public JsonList<T> add(@Nullable JsonList<T> left, @Nullable JsonList<T> right) {
        JsonList<T> result = new JsonList<T>(Converters.checkNull(left));
        result.addAll(right);
        return result;
    }
}

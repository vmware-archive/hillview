/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import javax.annotation.Nullable;

/**
 * This class contains a few nicer casts that can be inherited.
 */
public interface ICast {
    @Nullable
    default <T> T as(Class<T> clazz) {
        try {
            return clazz.cast(this);
        } catch (ClassCastException e) {
            return null;
        }
    }

    default <T> T as(Class<T> clazz, @Nullable String failureMessage) {
        T result = this.as(clazz);
        if (result == null) {
            if (failureMessage == null)
                failureMessage = this.getClass().getName() + " is not an instance of " + clazz.toString();
            throw new RuntimeException(failureMessage);
        }
        return result;
    }

    default <T> T to(Class<T> clazz) {
        return this.as(clazz, null);
    }
}

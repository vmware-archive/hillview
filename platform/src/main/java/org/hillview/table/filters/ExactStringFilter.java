/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.table.filters;

import org.hillview.table.api.IStringFilter;

import javax.annotation.Nullable;

/**
 * Returns true if a string equals a given string.
 */
public class ExactStringFilter implements IStringFilter {
    @Nullable
    final String toFind;
    final boolean caseSensitive;

    ExactStringFilter(@Nullable String toFind, final boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        if (!caseSensitive && toFind != null)
            toFind = toFind.toLowerCase();
        this.toFind = toFind;
    }

    @Override
    public boolean test(@Nullable String s) {
        if (this.toFind == null)
            return s == null;
        if (s == null)
            return false;
        if (!this.caseSensitive)
            s = s.toLowerCase();
        return this.toFind.equals(s);
    }
}

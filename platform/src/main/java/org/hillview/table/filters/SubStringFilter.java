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
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * Returns true if a string contains a given string.
 */
public class SubStringFilter implements IStringFilter {
    final String toFind;
    final boolean caseSensitive;

    SubStringFilter(@Nullable String toFind, boolean caseSensitive) {
        // we do not allow null sub-strings
        this.caseSensitive = caseSensitive;
        String f = Converters.checkNull(toFind);
        if (!caseSensitive)
            f = f.toLowerCase();
        this.toFind = f;
    }

    @Override
    public boolean test(@Nullable String s) {
        if (s == null)
            return false;
        if (!this.caseSensitive)
            s = s.toLowerCase();
        return s.contains(this.toFind);
    }
}

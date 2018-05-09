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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RegexStringFilter implements IStringFilter {
    private final Pattern pattern;

    public RegexStringFilter(String pattern, boolean caseSensitive) {
        this.pattern = Pattern.compile(pattern, caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
    }

    @Override
    public boolean test(@Nullable String s) {
        if (s == null)
            /// The null string does not match anything.
            return false;
        Matcher m = this.pattern.matcher(s);
        return m.matches();
    }
}

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
import java.io.Serializable;

/**
 * Description of a string filtering operation.
 * Not all combinations of options make sense.
 */
public class StringFilterDescription implements Serializable {
    /**
     * String to look for.
     */
    @Nullable
    private final String toFind;
    /**
     * If true the comparison will be made case-insensitive.
     */
    private final boolean caseSensitive;
    /**
     * If true the toFind string is interpreted as a regular expression.
     */
    private final boolean asRegex;
    /**
     * If true the search will match substrings.
     */
    private final boolean subString;

    public StringFilterDescription(@Nullable String toFind, boolean caseSensitive,
                                   boolean asRegex, boolean subString) {
        this.toFind = toFind;
        this.caseSensitive = caseSensitive;
        this.asRegex = asRegex;
        this.subString = subString;
    }

    public IStringFilter getFilter() {
        if (this.asRegex) {
            assert this.toFind != null;
            return new RegexStringFilter(this.toFind, this.caseSensitive);
        }
        if (this.subString)
            return new SubStringFilter(this.toFind, this.caseSensitive);
        return new ExactStringFilter(this.toFind, this.caseSensitive);
    }
}

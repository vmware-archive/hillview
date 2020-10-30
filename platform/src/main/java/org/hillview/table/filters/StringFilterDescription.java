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


package org.hillview.table.filters;

import javax.annotation.Nullable;
import java.io.Serializable;
/**
 * Description of a string filtering operation.
 * Not all combinations of options make sense.
 */
public class StringFilterDescription implements Serializable {
    static final long serialVersionUID = 1;

    /**
     * Search pattern to look for.
     */
    @Nullable
    public String compareValue;
    /**
     * If true the search pattern will be matched against substrings.
     */
    public boolean asSubString;
    /**
     * If true the search pattern will be interpreted as a regular expression.
     */
    public boolean asRegEx;
    /**
     * If true the comparison will be made case-sensitive.
     */
    public boolean caseSensitive;
    /**
     * If true all matches will be omitted.
     */
    public boolean complement;
    /**
     * If true skip the first row.
     */
    public boolean excludeTopRow;
    /**
     * If true then search forward, else search backwards.
     */
    public boolean next;

    public StringFilterDescription(
            @Nullable String compareValue,  boolean asSubString, boolean asRegEx,
            boolean caseSensitive, boolean complement, boolean excludeTopRow, boolean next) {
        this.compareValue = compareValue;
        this.asSubString = asSubString;
        this.asRegEx = asRegEx;
        this.caseSensitive = caseSensitive;
        this.complement = complement;
        this.excludeTopRow = excludeTopRow;
        this.next = next;
    }

    public StringFilterDescription(@Nullable String compareValue) {
        this(compareValue, false, false, false, false, false, false);
    }
}

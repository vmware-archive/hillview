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
    /**
     * Search pattern to look for.
     */
    @Nullable
    public final String compareValue;
    /**
     * If true the search pattern will be matched against substrings.
     */
    public final boolean asSubString;
    /**
     * If true the search pattern will be interpreted as a regular expression.
     */
    public final boolean asRegEx;
    /**
     * If true the comparison will be made case-sensitive.
     */
    public final boolean caseSensitive;
    /**
     * If true all matches will be omitted.
     */
    public final boolean complement;

    public StringFilterDescription(
            @Nullable String compareValue,  boolean asSubString, boolean asRegEx,
            boolean caseSensitive, boolean complement) {
        this.compareValue = compareValue;
        this.asSubString = asSubString;
        this.asRegEx = asRegEx;
        this.caseSensitive = caseSensitive;
        this.complement = complement;
    }

    public StringFilterDescription(@Nullable String compareValue,  boolean asSubString, boolean asRegEx,
            boolean caseSensitive) {
        this(compareValue, asSubString, asRegEx, caseSensitive, false);
    }

    public StringFilterDescription(@Nullable String compareValue) {
        this(compareValue, false, false, false, false);
    }
}


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

import org.apache.commons.lang.StringUtils;
import org.hillview.table.api.IStringFilter;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class StringFilterFactory {
    protected final StringFilterDescription stringFilterDescription;
    protected static IStringFilter instance = null;

    public StringFilterFactory(StringFilterDescription stringFilterDescription) {
        this.stringFilterDescription = stringFilterDescription;
        if (this.stringFilterDescription.compareValue == null)
            this.instance= new MissingValuesFilter();
        else {
            if (stringFilterDescription.asRegEx) {
                this.instance = new RegExFilter();
            } else if (stringFilterDescription.asSubString) {
                this.instance= new SubStringFilter();
            } else
                this.instance = new ExactCompFilter();
        }
    }

    public IStringFilter getFilter() {
        return this.instance;
    }

    class MissingValuesFilter implements IStringFilter {
        public boolean test(@Nullable String curString) {
            return (curString == null)^StringFilterFactory.this.stringFilterDescription.complement;
        }
    }

    class RegExFilter implements IStringFilter {
        private final Pattern regEx;

        public RegExFilter(){
            this.regEx = Pattern.compile(
                    StringFilterFactory.this.stringFilterDescription.compareValue,
                    StringFilterFactory.this.stringFilterDescription.caseSensitive ?
                            0 : Pattern.CASE_INSENSITIVE);
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && this.regEx.matcher(curString).matches();
            return result^StringFilterFactory.this.stringFilterDescription.complement;
        }
    }

    class SubStringFilter implements IStringFilter {
        private final String compareTo;

        public SubStringFilter() {
            this.compareTo = StringFilterFactory.this.stringFilterDescription.caseSensitive ?
                    StringFilterFactory.this.stringFilterDescription.compareValue :
                    StringFilterFactory.this.stringFilterDescription.compareValue.toLowerCase();
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && (
                    StringFilterFactory.this.stringFilterDescription.caseSensitive ?
                    curString.contains(this.compareTo) :
                    StringUtils.containsIgnoreCase(curString, this.compareTo));
            return result^StringFilterFactory.this.stringFilterDescription.complement;
        }
    }

    class ExactCompFilter implements IStringFilter {
        private final String compareTo;

        public ExactCompFilter() {
            this.compareTo = StringFilterFactory.this.stringFilterDescription.caseSensitive ?
                    StringFilterFactory.this.stringFilterDescription.compareValue :
                    StringFilterFactory.this.stringFilterDescription.compareValue.toLowerCase();
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && (
                    StringFilterFactory.this.stringFilterDescription.caseSensitive ?
                    curString.equals(this.compareTo) :
                    curString.equalsIgnoreCase(this.compareTo));
            return result^StringFilterFactory.this.stringFilterDescription.complement;
        }
    }
}

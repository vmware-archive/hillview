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
    public static IStringFilter instance = null;

    public IStringFilter getFilter(StringFilterDescription stringFilterDescription) {
        if (stringFilterDescription.compareValue == null)
            this.instance= new MissingValuesFilter(stringFilterDescription);
        else {
            if (stringFilterDescription.asRegEx) {
                this.instance = new RegExFilter(stringFilterDescription);
            } else if (stringFilterDescription.asSubString) {
                this.instance= new SubStringFilter(stringFilterDescription);
            } else
                this.instance = new ExactCompFilter(stringFilterDescription);
        }
        return this.instance;
    }

    class MissingValuesFilter implements IStringFilter {
        private StringFilterDescription stringFilterDescription;

        public MissingValuesFilter(StringFilterDescription stringFilterDescription) {
            this.stringFilterDescription = stringFilterDescription;
        }

        public boolean test(@Nullable String curString) {
            return (curString == null)^this.stringFilterDescription.complement;
        }
    }

    class RegExFilter implements IStringFilter {
        private StringFilterDescription stringFilterDescription;
        private final Pattern regEx;

        public RegExFilter(StringFilterDescription stringFilterDescription){
            this.stringFilterDescription = stringFilterDescription;
            this.regEx = Pattern.compile(
                    this.stringFilterDescription.compareValue,
                    this.stringFilterDescription.caseSensitive ? 0 : Pattern.CASE_INSENSITIVE);
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && this.regEx.matcher(curString).matches();
            return result^this.stringFilterDescription.complement;
        }
    }

    class SubStringFilter implements IStringFilter {
        private StringFilterDescription stringFilterDescription;
        private final String compareTo;

        public SubStringFilter(StringFilterDescription stringFilterDescription) {
            this.stringFilterDescription = stringFilterDescription;
            this.compareTo = stringFilterDescription.caseSensitive ?
                    stringFilterDescription.compareValue :
                    stringFilterDescription.compareValue.toLowerCase();
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && (
                    this.stringFilterDescription.caseSensitive ?
                    curString.contains(this.compareTo) :
                    StringUtils.containsIgnoreCase(curString, this.compareTo));
            return result^this.stringFilterDescription.complement;
        }
    }

    class ExactCompFilter implements IStringFilter {
        private StringFilterDescription stringFilterDescription;
        private final String compareTo;

        public ExactCompFilter(StringFilterDescription stringFilterDescription) {
            this.stringFilterDescription = stringFilterDescription;
            this.compareTo = stringFilterDescription.caseSensitive ?
                    stringFilterDescription.compareValue :
                    stringFilterDescription.compareValue.toLowerCase();
        }

        public boolean test(@Nullable String curString) {
            boolean result = (curString != null) && (
                    this.stringFilterDescription.caseSensitive ?
                    curString.equals(this.compareTo) :
                    curString.equalsIgnoreCase(this.compareTo));
            return result^this.stringFilterDescription.complement;
        }
    }
}

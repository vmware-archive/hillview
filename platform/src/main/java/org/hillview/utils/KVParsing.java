/*
 * Copyright (c) 2021 VMware Inc. All Rights Reserved.
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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility parameterized class that knows how to parse a
 * string into a sequence of Key-Value pairs.
 */
public class KVParsing implements IKVParsing {
    final Pattern kvRegex;
    final Pattern sepRegex;
    final Pattern wrapperRegex;

    /**
     * Create a class to parse a string into a sequence of key-value pairs.
     * The way this operators works is the following:
     * - first wrapperRegex is applied together with (^$) to force matching the whole string.
     * If it doesn't match the string does not parse as key-value pairs.
     * - 1. if wrapperRegex matches the string matching the first group is extracted
     * - 2. As long as the string is not empty the following is repeated:
     * - 3. The kvRegex(.*) is matched at the left of the string (using ^).
     * - 4. If it matches the key and values are extracted from group(1) and group(2).
     * - 5. Then the remainder of the string is matched with the sepRegex.
     * - 6. The remainder of the string is used to repeat from step 2.
     * For example, the following parameters:
     * kvRegex="(\S+)="[^"]*"
     * sepRegex=",\s*"
     * wrapperRegex="\[(.*)\]"
     * This will match a string like
     * [key="value", key1="value with spaces"]
     * @param kvRegex   Regular expression that contains two groups: first for key, second for value.
     * @param sepRegex  Regular expression that counsumer separators.
     * @param wrapperRegex  A regular expression that describes how the whole string is wrapped.
     *                      The regex should contain a single group that will produce the string used for matchin.
     */
    public KVParsing(String kvRegex, String sepRegex, String wrapperRegex) {
        this.kvRegex = Pattern.compile("^" + kvRegex + "(.*)$");
        this.sepRegex = Pattern.compile("^" + sepRegex + "(.*)$");
        this.wrapperRegex = Pattern.compile("^" + wrapperRegex + "$");
    }

    /**
     * Parse the specified string; pass the data to the indicated function.
     * @param data      String to parse.
     * @param consumer  Consumer of the data.  If it returns 'true' we stop parsing.
     * @return          The number of key-value pairs produced.
     */
    public int parse(@Nullable String data, BiFunction<String, String, Boolean> consumer) {
        // TODO: this function allocates a lot of strings, it would work much
        // better with something like string slices.
        int matches = 0;
        if (data == null)
            return matches;
        Matcher m = this.wrapperRegex.matcher(data);
        if (!m.matches())
            return matches;
        String remainder = m.group(1);
        while (remainder != null) {
            Matcher km = this.kvRegex.matcher(remainder);
            if (km.matches()) {
                String key = km.group(1);
                String value = km.group(2);
                boolean done = consumer.apply(key, value);
                matches++;
                if (done)
                    break;
                remainder = km.group(3);
            } else {
                break;
            }
            Matcher ks = this.sepRegex.matcher(remainder);
            if (ks.matches()) {
                remainder = ks.group(1);
            } else {
                break;
            }
        }
        return matches;
    }

    /**
     * Extract all keys from a string.
     * @param data      String to extract keys from.
     * @param consumer  Function the receives each key.
     * @return  Number of extracted keys.
     */
    public int extractKeys(@Nullable String data, Consumer<String> consumer) {
        return this.parse(data, (k, v) -> { consumer.accept(k); return false; });
    }

    /**
     * Extract the value associated with a key.
     * @param data  String to extract value from.
     * @param key   Key that the value is associated with.
     * @return      The associated value, or null of no value is found.
     */
    @Nullable
    public String extractValue(@Nullable String data, String key) {
        String[] result = new String[1];
        result[0] = null;
        this.parse(data, (k, v) -> {
            if (key.equals(k)) {
                result[0] = v;
                return false;
            } else {
                return true;
            }
        });
        return result[0];
    }

    /**
     * @return A parser suitable for decomposing the structured data field of an RFC5424 log line into key-value pairs.
     */
    public static KVParsing createRFC5424StructuredDataParser() {
        return new KVParsing("([a-zA-Z]+)=\"([^\"]*)\"",
                "\\s+",
                "\\[\\S+\\s+(.*)\\]");
    }

    /**
     * @return A parser suitable for decomposing a string with a structure approximating http headers.
     */
    public static KVParsing createHttpHeaderParser() {
        return new KVParsing("'(\\S+)=\"([^\"]*)\"'",
                ",\\s+",
                "\\[(.*)\\]");
    }
}

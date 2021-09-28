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

/**
 * Interface that knows how to parse a string into a sequence of key-value pairs.
 */
public interface IKVParsing {
    /**
     * Parse the specified string; pass the data to the indicated function.
     * @param data      String to parse.
     * @param consumer  Consumer of the data.  If it returns 'true' we stop parsing.
     * @return          The number of key-value pairs produced.
     */
    int parse(@Nullable String data, BiFunction<String, String, Boolean> consumer);

    /**
     * Extract all keys from a string.
     * @param data      String to extract keys from.
     * @param consumer  Function the receives each key.
     * @return  Number of extracted keys.
     */
    default int extractKeys(@Nullable String data, Consumer<String> consumer) {
        return this.parse(data, (k, v) -> { consumer.accept(k); return false; });
    }

    /**
     * Extract the value associated with a key.
     * @param data  String to extract value from.
     * @param key   Key that the value is associated with.
     * @return      The associated value, or null of no value is found.
     */
    @Nullable
    default String extractValue(@Nullable String data, String key) {
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
}

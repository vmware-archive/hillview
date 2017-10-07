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

package org.hillview.table.columns;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * This class is used to compress categorical data.
 */
public class CategoryEncoding {
    // Map categorical value to a small integer
    private final Object2IntOpenHashMap<String> encoding;
    // Decode small integer into categorical value
    private final Int2ObjectOpenHashMap<String> decoding;

    CategoryEncoding() {
        this.encoding = new Object2IntOpenHashMap<String>(100);
        this.decoding = new Int2ObjectOpenHashMap<String>(100);
    }

    @Nullable
    String decode(int code) {
        if (this.decoding.containsKey(code))
            return this.decoding.get(code);
        return null;
    }

    int encode(@Nullable String value) {
        if (this.encoding.containsKey(value))
            return this.encoding.getInt(value);
        int encoding = this.encoding.size();
        this.encoding.put(value, encoding);
        this.decoding.put(encoding, value);
        return encoding;
    }

    public void allDistinctStrings(Consumer<String> action) {
        this.encoding.forEach((k, v) -> action.accept(k));
    }
}

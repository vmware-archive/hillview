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

import it.unimi.dsi.fastutil.bytes.Byte2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ByteMap;
import it.unimi.dsi.fastutil.objects.Object2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * This class is used to compress categorical data.
 */
public class CategoryEncoding {
    // Map categorical value to a small integer
    private final Object2IntOpenHashMap<String> intEncoding;
    // Decode small integer into categorical value
    private final Int2ObjectOpenHashMap<String> intDecoding;
    // Map categorical value to a byte
    private final Object2ByteOpenHashMap<String> byteEncoding;
    // Decode byte into categorical value
    private final Byte2ObjectOpenHashMap<String> byteDecoding;


    CategoryEncoding() {
        this.intEncoding = new Object2IntOpenHashMap<String>(100);
        this.intDecoding = new Int2ObjectOpenHashMap<String>(100);
        this.byteEncoding = new Object2ByteOpenHashMap<String>(100);
        this.byteDecoding = new Byte2ObjectOpenHashMap<String>(100);
    }

    @Nullable
    String decode(int code) { return this.intDecoding.getOrDefault(code, null); }

    @Nullable
    String decode(byte code) { return this.byteDecoding.getOrDefault(code, null); }

    int encodeInt(@Nullable String value) {
        if (this.intEncoding.containsKey(value))
            return this.intEncoding.getInt(value);
        int encoding = this.intEncoding.size();
        this.intEncoding.put(value, encoding);
        this.intDecoding.put(encoding, value);
        return encoding;
    }

    byte encodeByte(@Nullable String value) {
        if (this.byteEncoding.containsKey(value))
            return this.byteEncoding.getByte(value);
        byte encoding = (byte) this.byteEncoding.size();
        this.byteEncoding.put(value, encoding);
        this.byteDecoding.put(encoding, value);
        return encoding;
    }

    public void allDistinctStrings(Consumer<String> action) {
        this.byteEncoding.keySet().forEach(action);
        this.intEncoding.keySet().forEach(action);
    }

    public boolean IsByteFull() {
        return (byteEncoding.size() >= 255);
    }
}

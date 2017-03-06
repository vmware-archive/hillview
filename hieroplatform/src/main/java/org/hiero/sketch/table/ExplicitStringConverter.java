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
 *
 */

package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IStringConverter;

import java.util.HashMap;

/**
 * A string converter which uses an explicit hash table to map strings to integers.
 */
public final class ExplicitStringConverter implements IStringConverter {
    private final HashMap<String, Integer> stringValue;

    public ExplicitStringConverter() {
        this.stringValue = new HashMap<String, Integer>();
    }

    /* Will throw an exception when string is not known */
    @Override
    public double asDouble(final String string) {
        return this.stringValue.get(string);
    }

    public void set(final String s, final int value) {
        this.stringValue.put(s, value);
    }
}

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

package org.hillview.table;

import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.IStringConverterDescription;

import java.util.HashMap;

/**
 * A string converter which uses an explicit hash table to map strings to integers.
 */
public abstract class BaseExplicitConverter implements IStringConverter, IStringConverterDescription {
    final HashMap<String, Integer> stringValue;

    BaseExplicitConverter() {
        this.stringValue = new HashMap<String, Integer>();
    }

    public void set(final String s, final int value) {
        this.stringValue.put(s, value);
    }

    @Override
    public IStringConverter getConverter() { return this; }
}

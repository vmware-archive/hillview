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

import javax.annotation.Nullable;

public class RadixConverter implements IStringConverter, IStringConverterDescription {
    // TODO: handle utf-8
    @Override
    public double asDouble(@Nullable String string) {
        double value = 0;
        double coefficient = 1;

        if (string != null) {
            for (int i = 0; i < string.length(); i++) {
                char c = string.charAt(i);
                value += (int) c * coefficient;
                coefficient /= 256;
            }
        }
        return value;
    }

    @Override
    public IStringConverter getConverter() {
        return this;
    }
}

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

package org.hillview.table.api;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.io.Serializable;

public class ColumnAndConverterDescription implements Serializable, IJson {
    public final String columnName;
    @Nullable
    public final IStringConverterDescription converter;

    public ColumnAndConverterDescription(String columnName,
                                         @Nullable
                                         IStringConverterDescription converter) {
        this.columnName = columnName;
        this.converter = converter;
    }

    /**
     * Create ColumnAndConverterDescription when no string converters are needed.
     */
    public static ColumnAndConverterDescription[] create(String[] colNames) {
        ColumnAndConverterDescription[] result =
                new ColumnAndConverterDescription[colNames.length];
        for (int i=0; i < colNames.length; i++)
            result[i] = new ColumnAndConverterDescription(colNames[i]);
        return result;
    }

    public ColumnAndConverterDescription(String columnName) {
        this(columnName, null);
    }

    @Nullable
    public IStringConverter getConverter() {
        if (this.converter == null)
            return null;
        return this.converter.getConverter();
    }
}

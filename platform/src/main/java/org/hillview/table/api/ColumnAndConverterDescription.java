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
import org.hillview.table.NoStringConverter;
import org.hillview.utils.Linq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ColumnAndConverterDescription implements Serializable, IJson {
    public final String columnName;
    public final IStringConverterDescription converter;

    public ColumnAndConverterDescription(String columnName,
                                         IStringConverterDescription converter) {
        this.columnName = columnName;
        this.converter = converter;
    }

    /**
     * Create ColumnAndConverterDescription when no string converters are needed.
     */
    public static List<ColumnAndConverterDescription> create(List<String> colNames) {
        return Linq.map(colNames, ColumnAndConverterDescription::new);
    }

    /**
     * Create ColumnAndConverterDescription when no string converters are needed.
     */
    public static List<ColumnAndConverterDescription> create(String[] colNames) {
        List<ColumnAndConverterDescription> result = new ArrayList<ColumnAndConverterDescription>
                (colNames.length);
        for (String c : colNames)
            result.add(new ColumnAndConverterDescription(c));
        return result;
    }

    public ColumnAndConverterDescription(String columnName) {
        this(columnName, NoStringConverter.getDescriptionInstance());
    }

    public IStringConverter getConverter() {
        return this.converter.getConverter();
    }
}

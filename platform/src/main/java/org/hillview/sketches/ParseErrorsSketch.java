/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;

/**
 * A sketch that computes the number of parsing errors in each column.
 */
public class ParseErrorsSketch implements ISketch<ITable, ParseErrorsSketch.PerColumnErrors> {
    static final long serialVersionUID = 1;
    @Override
    public PerColumnErrors create(@Nullable ITable data) {
        List<IColumn> columns = Converters.checkNull(data).getColumns(data.getSchema());
        PerColumnErrors result = new PerColumnErrors();
        for (IColumn col: columns)
            result.errorsPerColumn.put(col.getName(), (long)col.getParsingExceptionCount());
        return result;
    }

    @Nullable
    @Override
    public PerColumnErrors zero() {
        return new PerColumnErrors();
    }

    @Nullable
    @Override
    public PerColumnErrors add(@Nullable PerColumnErrors left, @Nullable PerColumnErrors right) {
        assert left != null;
        assert right != null;
        return left.add(right);
    }

    public static class PerColumnErrors implements IJson {
        static final long serialVersionUID = 1;
    
        HashMap<String, Long> errorsPerColumn = new HashMap<String, Long>();

        public PerColumnErrors add(PerColumnErrors other) {
            PerColumnErrors result = new PerColumnErrors();
            result.errorsPerColumn = new HashMap<String, Long>(this.errorsPerColumn);
            for (String col: other.errorsPerColumn.keySet()) {
                if (result.errorsPerColumn.containsKey(col))
                    result.errorsPerColumn.put(col,
                            result.errorsPerColumn.get(col) +
                            other.errorsPerColumn.get(col));
                else
                    result.errorsPerColumn.put(col,
                            other.errorsPerColumn.get(col));
            }
            return result;
        }
    }
}

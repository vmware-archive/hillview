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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.QuantizationSchema;
import org.hillview.table.QuantizedTable;
import org.hillview.table.api.ITableFilterDescription;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;

/**
 * A Map which implements table filtering: given a row index it returns true if the
 * row is in the resulting table.
 */
public class FilterMap implements IMap<ITable, ITable> {
    /**
     * Argument to the rowFilterPredicate.test method is a row index.
     * Returns true if a row has to be preserved
     */
    private final ITableFilterDescription rowFilterPredicate;
    @Nullable
    private final QuantizationSchema quantization;

    public FilterMap(ITableFilterDescription rowFilterPredicate,
                     @Nullable QuantizationSchema quantization) {
        this.rowFilterPredicate = rowFilterPredicate;
        this.quantization = quantization;
    }

    public FilterMap(ITableFilterDescription rowFilterPredicate) {
        this(rowFilterPredicate, null);
    }

    @Override
    public ITable apply(@Nullable ITable data) {
        Converters.checkNull(data);
        if (this.quantization != null)
            data = new QuantizedTable(data, this.quantization);
        ITableFilter filter = this.rowFilterPredicate.getFilter(data);
        HillviewLogger.instance.info("Filtering", "{0}", filter);
        IMembershipSet result = data.getMembershipSet().filter(filter::test);
        return data.selectRowsFromFullTable(result);
    }

    public String asString() {
        return this.rowFilterPredicate.toString();
    }
}

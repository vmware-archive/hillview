/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import org.hillview.dataset.api.IJson;
import org.hillview.table.columns.ColumnQuantization;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * A quantization schema has quantization information for each column.
 */
public class QuantizationSchema implements IJson, Serializable {
    // We use a LinkedHashMap for deterministic serialization
    private LinkedHashMap<String, ColumnQuantization> quantization;

    @Nullable
    private LinkedHashMap<String, Integer> columnIndexes;

    public QuantizationSchema() {
        this.quantization = new LinkedHashMap<String, ColumnQuantization>();
    }

    @Nullable
    public ColumnQuantization get(String colName) {
        return quantization.get(colName);
    }

    public Set<String> getColNames() {
        return quantization.keySet();
    }

    public void set(String col, ColumnQuantization quantization) {
        this.quantization.put(col, quantization);
        this.columnIndexes.put(col, columnIndexes.size());
    }

    public Integer getIndex(String colName) {
        initializeColumnIndexes();
        return columnIndexes.get(colName);
    }

    public void initializeColumnIndexes() {
        if (this.columnIndexes.size() > 0) { // Only need to initialize once
            return;
        }

        Set<String> colNames = this.getColNames();
        int i = 0;
        for (String x : colNames) {
            columnIndexes.put(x, i);
            i++;
        }
    }
}

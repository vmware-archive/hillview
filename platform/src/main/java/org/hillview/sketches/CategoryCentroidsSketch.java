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

package org.hillview.sketches;

import org.hillview.dataset.TableSketch;
import org.hillview.sketches.results.Centroids;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * For every unique category in the given column name, this sketch computes the centroid of all
 * rows belonging to that category. The centroids are defined in the nD space that is defined by
 * the columns that the given list of column names specifies.
 */
public class CategoryCentroidsSketch implements TableSketch<Centroids<String>> {
    static final long serialVersionUID = 1;
    private final String catColName;
    private final String[] columns;

    public CategoryCentroidsSketch(String catColumnName, String[] columns) {
        this.catColName = catColumnName;
        this.columns = columns;
    }

    @Override
    public Centroids<String> create(@Nullable ITable data) {
        List<String> allColumns = new ArrayList<String>(this.columns.length + 1);
        allColumns.add(this.catColName);
        allColumns.addAll(Arrays.asList(this.columns));

        List<IColumn> cols = Converters.checkNull(data).getLoadedColumns(allColumns);
        List<IColumn> numericColumns =
                new ArrayList<IColumn>(this.columns.length);
        for (int i=1; i < cols.size(); i++)
            numericColumns.add(cols.get(i));

        return new Centroids<String>(data.getMembershipSet(),
                cols.get(0)::asString, numericColumns);
    }

    @Override
    public Centroids<String> zero() {
        return new Centroids<String>();
    }

    @Override
    public Centroids<String> add(@Nullable Centroids<String> left, @Nullable Centroids<String> right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }
}

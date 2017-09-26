/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.sketches;

import java.util.List;

/**
 * For every unique category in the given column name, this sketch computes the centroid of all
 * rows belonging to that category. The centroids are defined in the nD space that is defined by
 * the columns that the given list of column names specifies.
 */
public class CategoryCentroidsSketch extends CentroidsSketch<String> {
    /**
     * @param catColumnName The name of the categorical column where we partition by.
     * @param columnNames The names of the columns that define the nD space where the centroids are computed.
     */
    public CategoryCentroidsSketch(String catColumnName, List<String> columnNames) {
        super((table, row) -> table.getColumn(catColumnName).asString(row), columnNames);
    }
}

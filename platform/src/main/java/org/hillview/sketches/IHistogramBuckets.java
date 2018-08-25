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

import org.hillview.table.api.IColumn;

import java.io.Serializable;

/**
 * A description of the buckets used to compute a histogram.
 */
public interface IHistogramBuckets extends Serializable {
    /**
     * Number of buckets; must be greater than 0.
     */
    int getNumOfBuckets();

    /**
     * @param column Column holding the data.
     * @param rowIndex  Index of the row in the column.
     * @return the index of the bucket in which the item should be placed.
     * If the value is out of the range of buckets this returns -1.
     */
    int indexOf(IColumn column, int rowIndex);
}

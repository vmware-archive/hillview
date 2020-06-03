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

package org.hillview.sketches.results;

import org.hillview.table.api.IColumn;

/**
 * Left endpoints for string buckets.
 */
public class StringHistogramBuckets extends ExplicitHistogramBuckets<String> {
    static final long serialVersionUID = 1;
    
    public StringHistogramBuckets(String column, String[] leftBoundaries, final String max) {
        super(column, leftBoundaries, max);
    }

    public StringHistogramBuckets(String column, final String[] leftBoundaries) {
        super(column, leftBoundaries);
    }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        String item = column.getString(rowIndex);
        if (item == null)
            // This should not really happen.
            return -1;
        return this.indexOf(item);
    }
}

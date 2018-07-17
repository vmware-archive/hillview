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
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * One-dimensional histogram
 */
public class NewHistogramSketch implements ISketch<ITable, NewHistogram> {
    private final IHistogramBuckets bucketDesc;
    private final String columnName;
    private final double rate;
    private final long seed;

    public NewHistogramSketch(IHistogramBuckets bucketDesc, String columnName,
                              double rate, long seed) {
        this.bucketDesc = bucketDesc;
        this.columnName = columnName;
        this.rate = rate;
        this.seed = seed;
    }

    @Override
    public NewHistogram create(final ITable data) {
        NewHistogram result = this.getZero();
        result.create(data.getLoadedColumn(this.columnName).column,
                data.getMembershipSet(),
                this.rate, this.seed, false);
        return result;
    }

    @Override
    public NewHistogram add(@Nullable final NewHistogram left,
                            @Nullable final NewHistogram right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }

    @Override
    public NewHistogram zero() {
        return new NewHistogram(this.bucketDesc);
    }
}

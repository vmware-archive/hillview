/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.jsonObjects;

import org.hillview.sketches.*;
import org.hillview.table.RadixConverter;
import org.hillview.table.SortedStringsConverterDescription;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.IStringConverterDescription;

import javax.annotation.Nullable;
import java.io.Serializable;

@SuppressWarnings("WeakerAccess,CanBeFinal")
public class ColumnAndRange implements Serializable {
    public String columnName = "";
    public boolean onStrings;  // If true the histogram is done on string values
    public double min;
    public double max;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns

    public ColumnAndConverterDescription getDescription() {
        IStringConverterDescription converter;
        if (this.bucketBoundaries != null) {
            converter = new SortedStringsConverterDescription(
                    this.bucketBoundaries, (int)Math.ceil(this.min), (int) Math.floor(this.max));
        } else {
            converter = new RadixConverter();
        }
        return new ColumnAndConverterDescription(this.columnName, converter);
    }

    public IBucketsDescription getBuckets(int count) {
        if (this.min >= this.max)
            count = 1;
        return new BucketsDescriptionEqSize(this.min, this.max, count);
    }

    public IHistogramBuckets getNewBuckets(int count) {
        if (this.min >= this.max)
            count = 1;
        if (this.onStrings) {
            assert this.bucketBoundaries != null;
            return new StringHistogramBuckets(this.bucketBoundaries);
        }
        return new DoubleHistogramBuckets(this.min, this.max, count);
    }
}

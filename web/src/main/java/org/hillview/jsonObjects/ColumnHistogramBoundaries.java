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

import javax.annotation.Nullable;
import java.io.Serializable;

@SuppressWarnings("WeakerAccess,CanBeFinal")
public class ColumnHistogramBoundaries implements Serializable {
    public String columnName = "";
    public boolean onStrings;  // If true the histogram is done on string values
    public double min;
    public double max;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns

    public IHistogramBuckets getBuckets(int count) {
        if (this.onStrings) {
            assert this.bucketBoundaries != null;
            return new StringHistogramBuckets(this.bucketBoundaries);
        }
        if (this.min >= this.max)
            count = 1;
        return new DoubleHistogramBuckets(this.min, this.max, count);
    }
}

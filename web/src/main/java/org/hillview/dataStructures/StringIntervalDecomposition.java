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

package org.hillview.dataStructures;

import org.hillview.sketches.results.StringHistogramBuckets;
import org.hillview.table.columns.StringColumnQuantization;

public class StringIntervalDecomposition extends IntervalDecomposition {
    public StringIntervalDecomposition(StringColumnQuantization quantization, StringHistogramBuckets buckets) {
        super(quantization, buckets.leftBoundaries.length);
        int previous = 0;
        for (int i = 0; i < buckets.getBucketCount(); i++) {
            String s = buckets.leftMargin(i);
            int si = this.quantization.bucketIndex(s);
            if (si >= 0 && previous >= 0 && si < previous)
                throw new RuntimeException("Indexed out of order: " + si + " < " + previous);
            previous = si;
            this.bucketQuantizationIndexes[i] = si;
        }
    }
}

/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import org.hillview.sketches.results.Count;
import org.hillview.sketches.results.IHistogramBuckets;

/**
 * 4D histogram computed using 4 nested GroupBySketch applications.
 */
public class Histogram4DSketch extends GroupBySketch<
        Groups<Groups<Groups<Count>>>,
        GroupByWorkspace<GroupByWorkspace<GroupByWorkspace<EmptyWorkspace>>>,
        Histogram3DSketch,
        Groups<Groups<Groups<Groups<Count>>>>> {

    public Histogram4DSketch(
            IHistogramBuckets buckets0,
            IHistogramBuckets buckets1,
            IHistogramBuckets buckets2,
            IHistogramBuckets buckets3) {
        super(buckets3, Groups::new,
                new Histogram3DSketch(buckets0, buckets1, buckets2));
    }
}

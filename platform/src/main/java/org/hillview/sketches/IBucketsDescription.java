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

import java.io.Serializable;

/**
 * Description of a one dimensional bucket set for computing histograms.
 * All buckets are left-inclusive and right-exclusive,
 * except the right most bucket which is right-inclusive.
 */
public interface IBucketsDescription extends Serializable {
    /**
     * Number of buckets; must be greater than 0.
     */
    int getNumOfBuckets();

    /**
     * @param index is a number in 0...numOfBuckets - 1
     * @return the left boundary of the bucket
     */
    double getLeftBoundary(int index);

    /**
     * @param index is a number in 0...numOfBuckets - 1
     * @return the right boundary of the bucket
     */
    double getRightBoundary(int index);

    /**
     * @param item is a double that will be placed in a bucket of the histogram
     * @return the index of the bucket in which the item should be placed.
     * All buckets are left-inclusive and right-exclusive, except the right most bucket which is right-inclusive.
     * If item is out of range of buckets returns -1
     */
    int indexOf(final double item);
}

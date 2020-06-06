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

package org.hillview.utils;

import org.hillview.dataset.api.ISketchResult;

import java.util.function.BiFunction;

/**
 * A group of values: one per bucket, plus one per missing bucket.
 * @param <R>  R values in group.
 */
public interface IGroup<R extends ISketchResult> extends ISketchResult {
    R getMissing();
    int size();
    /**
     * Get the bucket with the specified index.
     * @param index  Index. -1 for missing data.
     * @return       The data in the specified bucket.
     */
    R getBucket(int index);

    /**
     * Create a new group which contains the prefix sum of the existing group.
     * @param addition  Function used to add.
     * @param constructor Used to create the result.
     * @return  A group where the bucket i has the sum of buckets 0-i in the input.
     *          The missing bucket in the result has the sum of all buckets in the
     *          input, including the input missing bucket.
     */
    default <G extends IGroup<R>> G prefixSum(
            BiFunction<R, R, R> addition, BiFunction<JsonList<R>, R, G> constructor) {
        JsonList<R> perBucket = new JsonList<R>(this.size());
        R previous = null;
        for (int i = 0; i < this.size(); i++) {
            if (i == 0)
                previous = this.getBucket(i);
            else
                previous = addition.apply(previous, this.getBucket(i));
            perBucket.add(previous);
        }
        return constructor.apply(perBucket, addition.apply(previous, this.getMissing()));
    }
}

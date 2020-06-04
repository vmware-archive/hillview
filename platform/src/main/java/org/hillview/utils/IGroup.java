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

/**
 * A group of values: one per bucket, plus one per missing bucket.
 * @param <R>  R values in group.
 */
public interface IGroup<R> extends ISketchResult {
    R getMissing();
    int size();
    /**
     * Get the bucket with the specified index.
     * @param index  Index. -1 for missing data.
     * @return       The data in the specified bucket.
     */
    R getBucket(int index);
}

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

import org.hillview.utils.JsonList;
import javax.annotation.Nullable;

/**
 * Represents a set of strings that are quantiles from a sorted set of distinct strings.
 */
public class StringQuantiles extends BucketsInfo {
    public final JsonList<String> stringQuantiles;
    @Nullable
    public final String           maxBoundary;
    /**
     * True if the leftBoundaries contains all the strings in the underlying data set.
     */
    public final boolean          allStringsKnown;

    public StringQuantiles(
            JsonList<String> quantiles, @Nullable String max, boolean allStringsKnown,
            long presentCount, long missingCount) {
        this.stringQuantiles = quantiles;
        this.maxBoundary = max;
        this.allStringsKnown = allStringsKnown;
        this.presentCount = presentCount;
        this.missingCount = missingCount;
    }
}

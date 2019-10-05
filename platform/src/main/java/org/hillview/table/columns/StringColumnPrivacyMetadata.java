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

package org.hillview.table.columns;

import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * This class represents metadata used for computing differentially-private mechanisms
 * for columns storing strings.
 */
public class StringColumnPrivacyMetadata extends ColumnPrivacyMetadata {
    /**
     * Fixed global maximum value. Should be computable from
     * public information or otherwise uncorrelated with the data.
     */
    public final String globalMax;
    public final String[] leftBoundaries;

    /**
     * Create a privacy metadata for a string-type column.
     * @param epsilon      Differential privacy parameter.
     * @param leftBoundaries  Left boundaries of the string buckets.
     * @param globalMax    Maximum value expected in column.
     */
    public StringColumnPrivacyMetadata(double epsilon, String[] leftBoundaries, String globalMax) {
        super(epsilon);
        this.globalMax = globalMax;
        this.leftBoundaries = leftBoundaries;
        if (leftBoundaries.length == 0)
            throw new IllegalArgumentException("Empty left boundaries");
        if (leftBoundaries[leftBoundaries.length - 1].compareTo(globalMax) >= 0)
            throw new IllegalArgumentException("Max value is not larger than other values: " + globalMax);
        Utilities.checkSorted(leftBoundaries);
    }

    @Nullable
    public String roundDown(@Nullable String value) {
        if (value == null) return null;
        if (value.compareTo(this.globalMax) > 0)
            return this.globalMax;
        if (value.compareTo(this.leftBoundaries[0]) < 0)
            throw new RuntimeException("Value below the minimum");
        // This method returns index of the search key, if it is contained in the array,
        // else it returns (-(insertion point) - 1). The insertion point is the point
        // at which the key would be inserted into the array: the index of the first
        // element greater than the key, or a.length if all elements in the array
        // are less than the specified key.
        int index = Arrays.binarySearch(leftBoundaries, value);
        if (index < 0) {
            index = -index - 2;
        }
        return this.leftBoundaries[index];
    }
}

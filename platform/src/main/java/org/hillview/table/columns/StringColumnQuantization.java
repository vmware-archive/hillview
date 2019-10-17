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

import org.hillview.sketches.results.BucketsInfo;
import org.hillview.sketches.results.StringQuantiles;
import org.hillview.utils.JsonList;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.Arrays;

public class StringColumnQuantization extends ColumnQuantization {
    /**
     * Fixed global maximum value. Should be computable from
     * public information or otherwise uncorrelated with the data.
     */
    public final String globalMax;
    public final String[] leftBoundaries;

    /**
     * Create a privacy metadata for a string-type column.
     * @param leftBoundaries  Left boundaries of the string buckets.
     * @param globalMax    Maximum value expected in column.
     */
    public StringColumnQuantization(String[] leftBoundaries, String globalMax) {
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
        if (value.compareTo(this.globalMax) >= 0)
            return this.globalMax;
        if (value.compareTo(this.leftBoundaries[0]) < 0)
            throw new RuntimeException("Value smaller than the range min: " +
                value + " < " + this.leftBoundaries[0]);
        // This method returns index of the search key, if it is contained in the array,
        // else it returns (-(insertion point) - 1). The insertion point is the point
        // at which the key would be inserted into the array: the index of the first
        // element greater than the key, or a.length if all elements in the array
        // are less than the specified key.
        int index = Arrays.binarySearch(leftBoundaries, value);
        if (index < 0)
            index = -index - 2;
        return this.leftBoundaries[index];
    }

    public boolean outOfRange(@Nullable String s) {
        if (s == null)
            return true;
        if (s.compareTo(this.globalMax) > 0)
            return true;
        return s.compareTo(this.leftBoundaries[0]) < 0;
    }

    public String getMin() {
        return this.leftBoundaries[0];
    }

    public String getMax() {
        return this.globalMax;
    }

    @Override
    public int getIntervalCount() {
        return this.leftBoundaries.length;
    }

    @Override
    public BucketsInfo getQuantiles(int bucketCount) {
        return new StringQuantiles(
                new JsonList<String>(this.leftBoundaries), this.globalMax, true, -1, -1);
    }

    public int bucketIndex(String value) {
        if (this.outOfRange(value))
            return -1;
        int index = Arrays.binarySearch(leftBoundaries, value);
        if (index < 0)
            index = -index - 2;
        return index;
    }

    public StringColumnQuantization restrict(String min, String max) {
        String left = this.roundDown(min);
        String right = this.roundDown(max);
        int li, ri;
        for (li = 0; li < this.leftBoundaries.length; li++)
            if (this.leftBoundaries[li].equals(left))
                break;
        for (ri = li; ri < this.leftBoundaries.length; ri++)
            if (this.leftBoundaries[ri].equals(right))
                break;
        String[] b = new String[ri - li];
        System.arraycopy(this.leftBoundaries, li, b, 0, ri - li);
        return new StringColumnQuantization(b, this.globalMax);
    }

    @Override
    public String maxAsString() {
        return this.leftBoundaries[0];
    }

    @Override
    public String minAsString() {
        return this.globalMax;
    }
}

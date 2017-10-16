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

package org.hillview.table;

import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.IStringConverterDescription;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * This converter has a sorted array of strings defining some bucket boundaries and
 * a data range.  The first boundary corresponds to the minimum value, and the last
 * boundary corresponds to the max value.
 * We always expect that max - min >= boundaries.length.
 * The buckets have the left boundary inclusive and the right one exclusive,
 * except the last bucket which has the both boundaries inclusive.
 *
 * A string is converted to a number corresponding to the bucket in which it falls.
 * If a string is less than the first the result is less than min.
 * If a string is more than the last, the result is more than max.
 */
public class SortedStringsConverterDescription implements IStringConverterDescription {
    // Last boundary is inclusive
    private final String[] boundaries;
    private final int min;
    private final int max;

    public SortedStringsConverterDescription(String[] boundaries, int min, int max) {
        this.min = min;
        this.max = max;
        if (boundaries.length < 1)
            throw new RuntimeException("Cannot have empty boundaries");
        this.boundaries = boundaries;
    }

    public SortedStringsConverterDescription(String[] boundaries) {
        this(boundaries, 0, boundaries.length - 1);
    }

    @Override
    public IStringConverter getConverter() {
        return new Converter();
    }

    public class Converter implements IStringConverter {
        private final Object2DoubleOpenHashMap<String> memoizedResults;
        // The smallest value given by computeIndex is -1.
        private final double keyNotFound;

        public Converter() {
            this.memoizedResults = new Object2DoubleOpenHashMap<String>();
            this.keyNotFound = (boundaries.length) + 2;
        }

        @Override
        public double asDouble(@Nullable String string) {
            double index = memoizedResults.getOrDefault(string, keyNotFound);
            if (index < keyNotFound) {
                return index;
            }
            index = computeIndex(string);
            this.memoizedResults.put(string, index);
            return index;
        }

        public double computeIndex(@Nullable String string) {
            SortedStringsConverterDescription desc = SortedStringsConverterDescription.this;
            int index = Arrays.binarySearch(desc.boundaries, string);
            // This method returns index of the search key, if it is contained in the array,
            // else it returns (-(insertion point) - 1). The insertion point is the point
            // at which the key would be inserted into the array: the index of the first
            // element greater than the key, or a.length if all elements in the array
            // are less than the specified key.
            if (index < 0) {
                index = -index - 1;
                if (index == 0)
                    // before first element
                    index = -1;
            }
            if (desc.boundaries.length == 1)
                return desc.min + index;
            return desc.min + ((index * (double)(desc.max - desc.min)) / (desc.boundaries.length - 1));
        }
    }
}

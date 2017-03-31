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
 *
 */

package org.hiero.utils;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.IntArrayColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.utils.Randomness;

import java.security.InvalidParameterException;

/**
 * This class generates random columns of integers for testing.
 */
public class IntArrayGenerator {
    public static final ColumnDescription desc = new
            ColumnDescription("Identity", ContentsKind.Integer, true);

    public static IntArrayColumn generateIntArray(final int size) {
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, i);
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        return col;
    }

    public static IntArrayColumn getRandIntArray(final int size, final int range, final String name) {
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, false);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        final Randomness rn = Randomness.getInstance();
        for (int i = 0; i < size; i++)
            col.set(i, rn.nextInt(range));
        return col;
    }

    /**
     * Returns a column with a specified number of integers in the range
     * (1,..range), with the frequency of i proportional to base^i.
     * @param size the number of elements in the array.
     * @param base the base for the probabilities above.
     * @param range integers in the array lie in the interval (1,range)
     * @param name name of the column
     * @return An IntArray Column as described above.
     */
    public static IntArrayColumn getHeavyIntArray(final int size, final double base,
                                                  final int range, final String name) {
        if(base <= 1)
            throw new InvalidParameterException("Base should be  greater than 1.");
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, false);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        final Randomness rn = Randomness.getInstance();
        final int max = (int) Math.round(Math.pow(base,range));
        for (int i = 0; i < size; i++) {
            int j = rn.nextInt(max);
            int k = 0;
            while(j >= Math.pow(base,k))
                k++;
            col.set(i,k);
        }
        return col;
    }
}

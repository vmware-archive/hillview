/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.utils;

import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.api.ContentsKind;

import java.security.InvalidParameterException;

/**
 * This class generates random columns of integers for testing.
 */
public class IntArrayGenerator {

    /**
     * Generates a list of integers from {0,..., size -1}, every multiple of a specified modulus is
     * marked as missing.
     * @param name The name of the column.
     * @param size The size of the column.
     * @param mod Every index that is a multiple of this parameter is marked missing.
     * @return A column of integers.
     */
    public static IntArrayColumn getMissingIntArray(String name, final int size, int mod) {
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, true);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            if ((i % mod) == 0)
                col.setMissing(i);
            else
                col.set(i, i);
        }
        return col;
    }

    public static IntArrayColumn getRandIntArray(int size, int range,
                                                 String name, Randomness randomness) {
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, false);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++)
            col.set(i, randomness.nextInt(range));
        return col;
    }

    /**
     * Returns a column with a specified number of integers in the range
     * (1,..range), with the frequency of i proportional to base^i.
     * @param size the number of elements in the array.
     * @param base the base for the probabilities above.
     * @param range integers in the array lie in the interval (1,range)
     * @param name name of the column
     * @param rn   random number generator
     * @return An IntArray Column as described above.
     */
    public static IntArrayColumn getHeavyIntArray(final int size, final double base,
                                                  final int range, final String name,
                                                  Randomness rn) {
        if(base <= 1)
            throw new InvalidParameterException("Base should be  greater than 1.");
        final ColumnDescription desc = new ColumnDescription(name, ContentsKind.Integer, false);
        final IntArrayColumn col = new IntArrayColumn(desc, size);
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

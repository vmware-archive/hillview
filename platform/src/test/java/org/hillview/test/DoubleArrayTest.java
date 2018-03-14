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

package org.hillview.test;

import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.api.ContentsKind;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for DoubleArrayColumn class
 */
public class DoubleArrayTest extends BaseTest {
    private final int size = 100;
    private static final ColumnDescription desc = new
            ColumnDescription("SQRT", ContentsKind.Double);

    /**
     * Generates a double array with every fifth entry missing
     */
    public static DoubleArrayColumn generateDoubleArray(final int size, final int max) {
        return DoubleArrayTest.generateDoubleArray(size, max, 5);
    }

    /**
     * Generates a double array with every skip entry missing
     */
    public static DoubleArrayColumn generateDoubleArray(final int size, final int max, int
            skip) {
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.sqrt(i + 1) % max);
            if ((i % skip) == 0)
                col.setMissing(i);
        }
        return col;
    }

    private void checkContents(DoubleArrayColumn col) {
        for (int i = 0; i < col.sizeInRows(); i++) {
            assertEquals(Math.sqrt(i+1), col.getDouble(i), 1e-3);
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else
                assertFalse(col.isMissing(i));
        }
    }

    /* Test for constructor using length and no arrays*/
    @Test
    public void testDoubleArrayZero() {
        final DoubleArrayColumn col = generateDoubleArray(this.size, 100);
        assertEquals(col.sizeInRows(), this.size);
        this.checkContents(col);
    }

    /* Test for constructor using data array */
    @Test
    public void testDoubleArrayOne() {
        final double[] data = new double[this.size];
        for (int i = 0; i < this.size; i++)
            data[i] = Math.sqrt(i+1);
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, data);
        for (int i = 0; i < this.size; i++)
            if ((i % 5) == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), this.size);
        this.checkContents(col);
    }

    /* Test for constructor using two arrays: data and missing values*/
    @Test
    public void testDoubleArrayTwo() {
        final double[] data = new double[this.size];
        for (int i = 0; i < this.size; i++) {
            data[i] = Math.sqrt(i + 1);
        }
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, data);
        for (int i = 0; i < this.size; i++) {
            if ((i % 5) == 0)
                col.setMissing(i);
        }
        assertEquals(col.sizeInRows(), this.size);
        this.checkContents(col);
    }
}

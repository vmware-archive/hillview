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

package org.hillview.test;
import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.utils.Randomness;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

class TestUtil {

    private static void Percentiles(final long[] R1) {
        Arrays.sort(R1);
        System.out.println("Percentiles: 0,10,20,50,90,99 ");
        System.out.println(R1[0]+ " , " + R1[R1.length/10]+ " , " + R1[R1.length/5]+ " , "
                + R1[R1.length/2] + " , " + R1[(9 * R1.length) / 10] + " , " + R1[R1.length-1]);
    }

    /**
     *  takes a lambda and measures its running time num times,
       then prints a percentiles report */
    static public void runPerfTest(final Consumer<Integer> testing, final int num) {
        final int tmp = 0;
        long startTime, endTime;
        final long[] results = new long[num];
        for (int j=0; j < num; j++) {
            startTime = System.nanoTime();
            testing.accept(tmp);
            endTime = System.nanoTime();
            results[j] = endTime - startTime;
        }
        Percentiles(results);
    }

    /**
     * @param size the number of rows in the table
     * @return a table with size rows and 3 columns. A date column named "DOB". A string column
     * named "Name" and a double column named "Income".
     */
    static public Table createTable(int size) {
        final int numCols = 3;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(getRandDateArray(size, "DOB"));
        columns.add(getStringArray(size, "Name"));
        columns.add(generateDoubleArray(size, "Income"));
        final FullMembership full = new FullMembership(size);
        return new Table(columns, full);
    }

    static public SmallTable createSmallTable(int size) {
        final int numCols = 3;
        final List<IColumn> columns = new ArrayList<IColumn>(numCols);
        columns.add(getRandDateArray(size, "DOB"));
        columns.add(getStringArray(size, "Name"));
        columns.add(generateDoubleArray(size, "Income"));
        return new SmallTable(columns);
    }

    private static IColumn getRandDateArray(int size, String colName) {
        final ColumnDescription desc = new ColumnDescription(colName, ContentsKind.Date, false);
        LocalDateTime[] data = new LocalDateTime[size];
        final Randomness rn = new Randomness();
        for (int i = 0; i < size; i++) {
            data[i] =  LocalDateTime.of(1940 + rn.nextInt(70),
                    rn.nextInt(11) + 1, rn.nextInt(28) + 1, rn.nextInt(24), rn.nextInt(60));
        }
        return new DateArrayColumn(desc, data);
    }

    private static IColumn getStringArray(int size, String colName) {
        final ColumnDescription desc = new ColumnDescription(colName, ContentsKind.String, false);
        final StringArrayColumn col = new StringArrayColumn(desc, size);
        final String[] firstNames = new String[] {"Emma", "Noah", "Liam", "Olivia", "Mason",
                                            "Sophia", "Yossarian", "Jacob", "Emily", "Charlotte"};
        Randomness rn = new Randomness();
        for (int i = 0; i < size; i++) {
            int index = (rn.nextInt(10) + rn.nextInt(10)) / 2;
            col.set(i, firstNames[index]);
        }
        return col;
    }

    private static DoubleArrayColumn generateDoubleArray(final int size, String colName) {
        final ColumnDescription desc = new ColumnDescription(colName, ContentsKind.Double, false);
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        Randomness rn = new Randomness();
        for (int i = 0; i < size; i++) {
            col.set(i, Math.exp(rn.nextDouble()) * 100000);
        }
        return col;
    }
}

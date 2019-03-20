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
import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.table.columns.DateArrayColumn;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.utils.Randomness;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.function.Consumer;

public class TestUtil {
    private static final boolean silent = true;

    /**
     * Provides access to private members in classes for testing.
     */
    public static Object getPrivateField (Object o, String fieldName) {
        final Field[] fields = o.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (fieldName.equals(field.getName())) {
                try {
                    field.setAccessible(true);
                    return field.get(o);
                } catch (IllegalAccessException ex) {
                    Assert.fail("IllegalAccessException accessing " + fieldName);
                }
            }
        }
        Assert.fail ("Field '" + fieldName + "' not found");
        return null;
    }

    private static void Percentiles(final long[] R1) {
        Arrays.sort(R1);
        if (!silent) {
            System.out.println("Percentiles: 0,10,20,50,90,99 ");
            System.out.println(R1[0] + " , " + R1[R1.length / 10] + " , " + R1[R1.length / 5] + " , "
                    + R1[R1.length / 2] + " , " + R1[(9 * R1.length) / 10] + " , " + R1[R1.length - 1]);
        }
    }

    /**
     * Takes a lambda and measures its running time num times,
     * then prints a percentiles report.  TODO: should check for regressions automatically.
     **/
    public static void runPerfTest(String test, final Consumer<Integer> testing, final int num) {
        if (!silent)
            System.out.println(test);
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

    private static IColumn getRandDateArray(int size, String colName) {
        final ColumnDescription desc = new ColumnDescription(colName, ContentsKind.Date);
        Instant[] data = new Instant[size];
        final Randomness rn = new Randomness(0);
        for (int i = 0; i < size; i++) {
            data[i] =  LocalDateTime.of(1940 + rn.nextInt(70),
                    rn.nextInt(11) + 1, rn.nextInt(28) + 1, rn.nextInt(24), rn.nextInt(60))
                    .toInstant(ZoneOffset.UTC);
        }
        return new DateArrayColumn(desc, data);
    }

    private static IColumn getStringArray(int size, String colName) {
        final ColumnDescription desc = new ColumnDescription(colName, ContentsKind.String);
        final StringArrayColumn col = new StringArrayColumn(desc, size);
        final String[] firstNames = new String[] {"Emma", "Noah", "Liam", "Olivia", "Mason",
                                            "Sophia", "Yossarian", "Jacob", "Emily", "Charlotte"};
        Randomness rn = new Randomness(0);
        for (int i = 0; i < size; i++) {
            int index = (rn.nextInt(10) + rn.nextInt(10)) / 2;
            col.set(i, firstNames[index]);
        }
        return col;
    }

    private static DoubleArrayColumn generateDoubleArray(final int size, String colName) {
        final ColumnDescription desc = new ColumnDescription(colName, ContentsKind.Double);
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        Randomness rn = new Randomness(0);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.exp(rn.nextDouble()) * 100000);
        }
        return col;
    }

    /**
     * Compare current run time of test to saved run time.
     */
    @SuppressWarnings("EmptyMethod")
    private static void printPerf(final String testName, final long time) {
        // TODO
        /*System.out.println(testName + " took " + time/1000 + " us");*/
    }

    /**
     * Compare current run time of calling function to saved run time.
     * @param time This is the time taken by the calling function.
     */
    public static void comparePerf(final long time) {
        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        final int callerIndex = 2;
        final String name = stack[callerIndex].getClassName() + "." + stack[callerIndex].getMethodName();
        printPerf(name, time);
    }

    public static void comparePerf(final String printThis, final long time) {
        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        final int callerIndex = 2;
        final String name = stack[callerIndex].getClassName() + "."
                + stack[callerIndex].getMethodName() + printThis;
        printPerf(name, time);
    }
}

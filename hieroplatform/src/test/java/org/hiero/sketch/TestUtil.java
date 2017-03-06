/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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

package org.hiero.sketch;
import java.util.Arrays;
import java.util.function.Consumer;

class TestUtil {
    private static void Percentiles(final long[] R1) {
        Arrays.sort(R1);
        System.out.println("Percentiles: 0,10,20,50,90,99 ");
        System.out.println(R1[0]+ " , " + R1[R1.length/10]+ " , " + R1[R1.length/5]+ " , "
                + R1[R1.length/2] + " , " + R1[(9 * R1.length) / 10] + " , " + R1[R1.length-1]);
    }

    /* takes a lambda and measures its running time num times,
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
}

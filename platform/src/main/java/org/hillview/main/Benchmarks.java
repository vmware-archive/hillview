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

package org.hillview.main;

public class Benchmarks {
    private static long time(Runnable runnable) {
        long start = System.nanoTime();
        runnable.run();
        long end = System.nanoTime();
        return end - start;
    }

    private static String twoDigits(double d) {
        return String.format("%.2f", d);
    }

    protected static double runNTimes(Runnable runnable, int count, String message, long elemCount) {
        long[] times = new long[count];
        for (int i=0; i < count; i++) {
            long t = time(runnable);
            times[i] = t;
        }
        int minIndex = 0;
        for (int i=0; i < count; i++)
            if (times[i] < times[minIndex])
                minIndex = i;
        for (int i=0; i < count; i++) {
            double speed = (double)(elemCount) / (times[i] / 1000.0);
            double percent = 100 * ((double)times[i] - times[minIndex]) / times[minIndex];
            System.out.println(message + "," +
                    (times[i]/(1000.0 * 1000.0)) + "," +
                    twoDigits(speed) + "," +
                    twoDigits(percent) + "%");
        }
        return times[minIndex];
    }
}

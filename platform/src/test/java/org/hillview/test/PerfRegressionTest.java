/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

class PerfRegressionTest {
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
    static void comparePerf(final long time) {
        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        final int callerIndex = 2;
        final String name = stack[callerIndex].getClassName() + "." + stack[callerIndex].getMethodName();
        printPerf(name, time);
    }

    static void comparePerf(final String printThis, final long time) {
        final StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        final int callerIndex = 2;
        final String name = stack[callerIndex].getClassName() + "."
                + stack[callerIndex].getMethodName() + printThis;
        printPerf(name, time);
    }
}

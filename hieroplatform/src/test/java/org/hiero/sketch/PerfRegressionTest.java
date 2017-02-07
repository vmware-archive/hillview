package org.hiero.sketch;

class PerfRegressionTest {
    /**
     * Compare current run time of test to saved run time.
     */
    @SuppressWarnings("EmptyMethod")
    private static void printPerf(final String testName, final long time) {
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

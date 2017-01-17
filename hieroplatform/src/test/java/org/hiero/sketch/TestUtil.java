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

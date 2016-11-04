package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.HeapTopK;

public class HeapInsertTimed {
    static void runOnInp(final HeapTopK<Integer> myHeap, final int [] Inp) {
        final long startTime = System.nanoTime();
        for (final int j:Inp) {
            myHeap.push(j);
        }
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
    }
}

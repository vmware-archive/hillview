
package org.hiero.sketch;
import org.hiero.sketch.spreadsheet.TreeTopK;

public class TreeInsertTimed {
    static void runOnInp(final TreeTopK<Integer> myTree, final int [] Inp) {
        final long startTime = System.nanoTime();
        for (final int j:Inp) {
            myTree.push(j);
        }
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
    }
}

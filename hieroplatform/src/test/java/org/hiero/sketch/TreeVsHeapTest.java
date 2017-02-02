package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.HeapTopK;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.hiero.utils.Randomness;
import org.junit.Test;

public class TreeVsHeapTest {
    private final int inpSize = 1000;
    private final int[] randInp = new int[this.inpSize];
    private final Randomness rn = Randomness.getInstance();

    @Test
    public void TreeVsHeapOne() {
        final int runs = 10;
        long startTime, endTime;
        //noinspection ConstantConditions
        for (int i = 1; i < runs; i++) {
            for (int j = 1; j < this.inpSize; j++) {
                this.randInp[j] = this.rn.nextInt(this.inpSize);
            }
            final int maxSize = 100;
            final HeapTopK<Integer> myHeap = new HeapTopK<>(maxSize, MyCompare.instance);
            startTime = System.nanoTime();
            for (final int j: this.randInp) {
                myHeap.push(j);
            }
            endTime = System.nanoTime();
            PerfRegressionTest.comparePerf(" Using Heap: ", endTime - startTime);
            final TreeTopK<Integer> myTree = new TreeTopK<>(maxSize, MyCompare.instance);
            startTime = System.nanoTime();
            for (final int j: this.randInp) {
                myTree.push(j);
            }
            endTime = System.nanoTime();
            PerfRegressionTest.comparePerf(" Using Tree: ", endTime - startTime);
        }
    }
}

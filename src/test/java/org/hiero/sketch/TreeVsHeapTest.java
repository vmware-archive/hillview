package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.HeapTopK;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;
import java.util.Random;

public class TreeVsHeapTest {
    private final int inpSize = 1000;
    private final int[] randInp = new int[this.inpSize];
    private final Random rn = new Random();

    @Test
    public void TreeVsHeapOne() {
        final int runs = 1;
        long startTime, endTime;
        for (int i = 1; i < runs; i++) {
            for (int j = 1; j < this.inpSize; j++) {
                this.randInp[j] = this.rn.nextInt(this.inpSize);
            }
            final int maxSize = 100;
            final HeapTopK<Integer> myHeap = new HeapTopK<>(maxSize, MyCompare.instance);
            startTime = System.nanoTime();
            for (final int j:randInp) {
                myHeap.push(j);
            }
            endTime = System.nanoTime();
            PerfRegressionTest.comparePerf(" Using Heap: ", endTime - startTime);
            final TreeTopK<Integer> myTree = new TreeTopK<>(maxSize, MyCompare.instance);
            startTime = System.nanoTime();
            for (final int j:randInp) {
                myTree.push(j);
            }
            endTime = System.nanoTime();
            PerfRegressionTest.comparePerf(" Using Tree: ", endTime - startTime);
        }
    }
}

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
        for (int i = 1; i < 10; i++) {
            final int maxSize = 1000;
            final TreeTopK<Integer> myTree = new TreeTopK<Integer>(maxSize, MyCompare.instance);
            final HeapTopK<Integer> myHeap = new HeapTopK<Integer>(maxSize, MyCompare.instance);
            for (int j = 1; j < this.inpSize; j++) {
                this.randInp[j] = this.rn.nextInt(this.inpSize);
            }

            long startTime = System.nanoTime();
            for (int j = 1; j < this.inpSize; j++) {
                myHeap.push(this.randInp[j]);
            }
            long endTime = System.nanoTime();
            System.out.format("Run %d: Heap takes %d ms, ",i, (endTime - startTime)/1000000);

            startTime = System.nanoTime();
            for (int j = 1; j < this.inpSize; j++) {
                myTree.push(this.randInp[j]);
            }
            endTime = System.nanoTime();
            System.out.format(" Tree takes %d ms%n", (endTime - startTime)/1000000);
        }
    }
}

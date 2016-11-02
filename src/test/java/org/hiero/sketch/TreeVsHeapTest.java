package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.HeapTopK;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;
import java.util.Random;

public class TreeVsHeapTest {
    final int inpSize = 1000;
    private final int maxSize = 100;
    private int[] randInp = new int[inpSize];
    private TreeTopK<Integer> myTree;
    private HeapTopK<Integer> myHeap;
    final Random rn = new Random();
    private long startTime, endTime;

    @Test
    public void TreeVsHeapOne() {
        for (int i = 1; i < 1; i++) {
            myTree = new TreeTopK<Integer>(this.maxSize, MyCompare.instance);
            myHeap = new HeapTopK<Integer>(this.maxSize, MyCompare.instance);
            for (int j = 1; j < inpSize; j++) {
                randInp[j] = rn.nextInt(inpSize);
            }

            startTime = System.nanoTime();
            for (int j = 1; j < inpSize; j++) {
                this.myHeap.push(randInp[j]);
            }
            endTime = System.nanoTime();
            System.out.format("Run %d: Heap takes %d ms, ",i, (endTime - startTime)/1000000);

            startTime = System.nanoTime();
            for (int j = 1; j < inpSize; j++) {
                this.myTree.push(randInp[j]);
            }
            endTime = System.nanoTime();
            System.out.format(" Tree takes %d ms%n", (endTime - startTime)/1000000);
        }
    }
}

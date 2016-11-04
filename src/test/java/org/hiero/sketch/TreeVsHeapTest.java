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
        int runs = 5;
        for (int i = 1; i < runs; i++) {
            for (int j = 1; j < this.inpSize; j++) {
                this.randInp[j] = this.rn.nextInt(this.inpSize);
            }
            int maxSize = 100;
            final HeapTopK<Integer> myHeap = new HeapTopK<>(maxSize, MyCompare.instance);
            final TreeTopK<Integer> myTree = new TreeTopK<>(maxSize, MyCompare.instance);
            HeapInsertTimed.runOnInp(myHeap, this.randInp);
            TreeInsertTimed.runOnInp(myTree, this.randInp);
        }
    }
}

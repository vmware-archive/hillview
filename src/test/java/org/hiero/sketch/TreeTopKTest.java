package org.hiero.sketch;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;
import java.util.Random;

/**
 * Tests for TreeMap implementation f TopK.
 */
public class TreeTopKTest {
    private final int maxSize = 10;
    public final int inpSize = 1000;
    private final TreeTopK<Integer> myTree = new TreeTopK<Integer>(this.maxSize, MyCompare.instance);

    @Test
    public void testHeapTopKZero() {
        final Random rn =new Random();
        for (int j =1; j <20; j++) {
            for (int i = 1; i < 1000; i++)
                this.myTree.push(rn.nextInt(10000));
            //System.out.println(myHeap.getTopK().toString());
        }
    }

    @Test
    public void testTreeTopKTimed() {
        final Random rn = new Random();
        final int inpSize = 1000000;
        final long startTime = System.nanoTime();
        for (int j = 1; j < inpSize; j++)
            this.myTree.push(rn.nextInt(inpSize));
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
        //System.out.format("Largest: %d%n", myTree.getTopK().lastKey());
    }
}

package org.hiero.sketch;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;
import java.util.Random;
import static org.hiero.sketch.MyCompare.instance;

/**
 * Tests for TreeMap implementation f TopK.
 */
public class TreeTopKTest {
    public final int maxSize = 10;
    public final int inpSize = 1000;
    private TreeTopK<Integer> myTree = new TreeTopK<Integer>(this.maxSize, instance.reversed());

    @Test
    public void testHeapTopKZero() {
        Random rn =new Random();
        for (int j =1; j <20; j++) {
            for (int i = 1; i < 1000; i++)
                this.myTree.push(rn.nextInt(10000));
            //System.out.println(myHeap.getTopK().toString());
        }
    }

    @Test
    public void testTreeTopKTimed() {
        Random rn = new Random();
        long startTime = System.nanoTime();
        for (int j = 1; j < this.inpSize; j++)
            this.myTree.push(rn.nextInt(this.inpSize));
        long endTime = System.nanoTime();
        //System.out.format("Largest: %d%n", myTree.getTopK().lastKey());
        //System.out.format("Time taken by tree: %d%n", (endTime - startTime) / 1000000);
    }
}

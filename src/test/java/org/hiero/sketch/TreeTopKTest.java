package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;

import java.util.Random;

import static org.hiero.sketch.MyCompare.instance;

/**
 * Created by parik on 10/25/16.
 */
public class TreeTopKTest {
    public final int maxSize = 10;
    public final int inpSize = 1000;
    private TreeTopK<Integer> myTree = new TreeTopK<Integer>(maxSize, instance);

    @Test
    public void testTreeTopKZero() {
        Random rn = new Random();
        long startTime = System.nanoTime();
        for (int j = 1; j < inpSize; j++)
            myTree.push(rn.nextInt(inpSize));
        long endTime = System.nanoTime();
        //System.out.format("Largest: %d%n", myTree.getTopK().lastKey());
        //System.out.format("Time taken by tree: %d%n", (endTime - startTime) / 1000000);
    }
}

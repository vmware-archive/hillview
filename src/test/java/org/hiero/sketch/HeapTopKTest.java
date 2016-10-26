package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.HeapTopK;
import org.junit.Test;

import java.util.Random;

import static org.hiero.sketch.MyCompare.instance;

/**
 * Created by parik on 10/19/16.
 */
public class HeapTopKTest {
    public final int maxSize = 10;
    public final int inpSize = 1000;
    private HeapTopK<Integer> myHeap = new HeapTopK<Integer>(maxSize, instance);


    @Test
    public void testHeapTopKZero() {
        Random rn =new Random();
        for (int j =1; j <20; j++) {
            for (int i = 1; i < 1000; i++)
                myHeap.push(rn.nextInt(10000));
            //System.out.println(myHeap.getTopK().toString());
        }

        /*
        for(int i =1; i < 10000; i++)
            myHeap.push(rn.nextInt(10000));

        System.out.println(myMap.toString());
        */
    }

    @Test
    public void testHeapTopKTimed() {
        Random rn = new Random();
        long startTime = System.nanoTime();
        for (int j = 1; j < inpSize; j++)
            myHeap.push(rn.nextInt(inpSize));
        long endTime = System.nanoTime();
        //System.out.format("Largest: %d%n", myHeap.getTopK().lastKey());
        //System.out.format("Time taken by HashMap: %d%n", (endTime - startTime) / 1000000);
    }


}

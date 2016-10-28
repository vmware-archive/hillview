package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.MonoidTopK;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;
import java.util.Random;

public class TestMonoidTopK {
    public int maxSize =1000;
    public int lSize;
    public int rSize;
    public final int inpSize = 10000;
    private TreeTopK<Integer> leftTree;
    private TreeTopK<Integer> rightTree;
    MonoidTopK<Integer> myTopK = new MonoidTopK<Integer>(maxSize, MyCompare.instance);

    @Test
    public void MonoidTopKTestZero() {
        lSize = 100;
        rSize = 100;
        leftTree = new TreeTopK<Integer>(lSize, MyCompare.instance);;
        rightTree = new TreeTopK<Integer>(rSize, MyCompare.instance);
        Random rn = new Random();
        for (int i = 0; i < inpSize; i++)
            leftTree.push(rn.nextInt(inpSize));
        for (int j = 0; j < inpSize; j++)
            rightTree.push(rn.nextInt(inpSize));
        /*System.out.println(rightTree.getTopK().toString());
        System.out.println(leftTree.getTopK().toString());
        System.out.println(myTopK.Add(leftTree.getTopK(), rightTree.getTopK()).toString());*/
    }

    @Test
    public void MonoidTopKTestOne() {
        lSize = 50;
        rSize = 50;
        leftTree = new TreeTopK<Integer>(lSize, MyCompare.instance);
        rightTree = new TreeTopK<Integer>(rSize, MyCompare.instance);
        Random rn = new Random();
        for (int i = 0; i < inpSize; i++)
            leftTree.push(rn.nextInt(inpSize));
        for (int j = 0; j < inpSize; j++)
            rightTree.push(rn.nextInt(inpSize));
        //System.out.println(rightTree.getTopK().toString());
        //System.out.println(leftTree.getTopK().toString());
        //System.out.println(myTopK.Add(leftTree.getTopK(), rightTree.getTopK()).toString());
    }

    @Test
    public void MonoidTopKTestTimed() {
        lSize = 1000;
        rSize = 1000;
        leftTree = new TreeTopK<Integer>(lSize, MyCompare.instance);
        rightTree = new TreeTopK<Integer>(rSize, MyCompare.instance);
        Random rn = new Random();
        for (int i = 0; i < inpSize; i++)
            leftTree.push(rn.nextInt(inpSize));
        for (int j = 0; j < inpSize; j++)
            rightTree.push(rn.nextInt(inpSize));
        long startTime = System.nanoTime();
        myTopK.Add(leftTree.getTopK(), rightTree.getTopK());
        long endTime = System.nanoTime();
        //System.out.format("Time taken to merge: %d%n", (endTime - startTime) / 1000000);
    }
}


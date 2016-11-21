package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.MonoidTopK;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.junit.Test;
import java.util.Random;

public class TestMonoidTopK {
    private final int maxSize =1000;
    private int lSize;
    private int rSize;
    private final int inpSize = 10000;
    private TreeTopK<Integer> leftTree;
    private TreeTopK<Integer> rightTree;
    private final MonoidTopK<Integer> myTopK = new MonoidTopK<Integer>(this.maxSize, MyCompare.instance);

    @Test
    public void MonoidTopKTestZero() {
        this.lSize = 100;
        this.rSize = 100;
        this.leftTree = new TreeTopK<Integer>(this.lSize, MyCompare.instance);
        this.rightTree = new TreeTopK<Integer>(this.rSize, MyCompare.instance);
        final Random rn = new Random();
        for (int i = 0; i < this.inpSize; i++)
            this.leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            this.rightTree.push(rn.nextInt(this.inpSize));
        /*System.out.println(rightTree.getTopK().toString());
        System.out.println(leftTree.getTopK().toString());
        System.out.println(myTopK.Add(leftTree.getTopK(), rightTree.getTopK()).toString());*/
    }

    @Test
    public void MonoidTopKTestOne() {
        this.lSize = 50;
        this.rSize = 50;
        this.leftTree = new TreeTopK<Integer>(this.lSize, MyCompare.instance);
        this.rightTree = new TreeTopK<Integer>(this.rSize, MyCompare.instance);
        final Random rn = new Random();
        for (int i = 0; i < this.inpSize; i++)
            this.leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            this.rightTree.push(rn.nextInt(this.inpSize));
        //System.out.println(rightTree.getTopK().toString());
        //System.out.println(leftTree.getTopK().toString());
        //System.out.println(myTopK.Add(leftTree.getTopK(), rightTree.getTopK()).toString());
    }

    @Test
    public void MonoidTopKTestTimed() {
        this.lSize = 1000;
        this.rSize = 1000;
        this.leftTree = new TreeTopK<Integer>(this.lSize, MyCompare.instance);
        this.rightTree = new TreeTopK<Integer>(this.rSize, MyCompare.instance);
        final Random rn = new Random();
        for (int i = 0; i < this.inpSize; i++)
            this.leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            this.rightTree.push(rn.nextInt(this.inpSize));
        final long startTime = System.nanoTime();
        this.myTopK.add(this.leftTree.getTopK(), this.rightTree.getTopK());
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
        //System.out.format("Time taken to merge: %d%n", (endTime - startTime) / 1000000);
    }
}


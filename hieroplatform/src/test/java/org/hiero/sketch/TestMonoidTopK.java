package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.MonoidTopK;
import org.hiero.sketch.spreadsheet.TreeTopK;
import org.hiero.utils.Converters;
import org.hiero.utils.Randomness;
import org.junit.Test;

import java.util.SortedMap;
import static org.junit.Assert.assertTrue;


public class TestMonoidTopK {
    private final int maxSize =1000;
    private int lSize;
    private int rSize;
    private final int inpSize = 10000;
    private TreeTopK<Integer> leftTree;
    private TreeTopK<Integer> rightTree;
    private final MonoidTopK<Integer> myTopK = new MonoidTopK<Integer>(this.maxSize, MyCompare.instance);

    void checkSorted(SortedMap<Integer, Integer> t) {
        boolean first = true;
        int previous = 0;
        for (int k : t.keySet()) {
            if (!first)
                assertTrue(previous < k);
            previous = k;
            first = false;
        }
    }

    @Test
    public void MonoidTopKTest0() {
        this.lSize = 100;
        this.rSize = 100;
        this.leftTree = new TreeTopK<Integer>(this.lSize, MyCompare.instance);
        this.rightTree = new TreeTopK<Integer>(this.rSize, MyCompare.instance);
        final Randomness rn = Randomness.getInstance();
        for (int i = 0; i < this.inpSize; i++)
            this.leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            this.rightTree.push(rn.nextInt(this.inpSize));
        this.checkSorted(this.leftTree.getTopK());
        this.checkSorted(this.rightTree.getTopK());
        SortedMap<Integer, Integer> s = this.myTopK.add(this.leftTree.getTopK(), this.rightTree.getTopK());
        this.checkSorted(Converters.checkNull(s));
    }

    @Test
    public void MonoidTopKTest1() {
        this.lSize = 50;
        this.rSize = 50;
        this.leftTree = new TreeTopK<Integer>(this.lSize, MyCompare.instance);
        this.rightTree = new TreeTopK<Integer>(this.rSize, MyCompare.instance);
        final Randomness rn = Randomness.getInstance();
        for (int i = 0; i < this.inpSize; i++)
            this.leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            this.rightTree.push(rn.nextInt(this.inpSize));
        this.checkSorted(this.leftTree.getTopK());
        this.checkSorted(this.rightTree.getTopK());
        SortedMap<Integer, Integer> s = this.myTopK.add(this.leftTree.getTopK(), this.rightTree.getTopK());
        this.checkSorted(Converters.checkNull(s));
    }

    @Test
    public void MonoidTopKTestTimed() {
        this.lSize = 1000;
        this.rSize = 1000;
        this.leftTree = new TreeTopK<Integer>(this.lSize, MyCompare.instance);
        this.rightTree = new TreeTopK<Integer>(this.rSize, MyCompare.instance);
        final Randomness rn = Randomness.getInstance();
        for (int i = 0; i < this.inpSize; i++)
            this.leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            this.rightTree.push(rn.nextInt(this.inpSize));
        final long startTime = System.nanoTime();
        this.myTopK.add(this.leftTree.getTopK(), this.rightTree.getTopK());
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
    }
}


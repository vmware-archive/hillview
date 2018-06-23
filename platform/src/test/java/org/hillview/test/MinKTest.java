package org.hillview.test;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.BottomKSketch;
import org.hillview.sketches.MinKSet;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class MinKTest {

    @Test
    public void testStringTable() {
        int suppSize = 100000;
        int length = 6;
        List<String> randomString = TestTables.randStringArray(suppSize, length);
        int num = suppSize*((int) Math.ceil(Math.log(suppSize)));
        Pair<Table, SortedMap<String, Integer>> pair = TestTables.randStringTable(num, randomString);
        int numSamples = 10000;
        BottomKSketch bks = new BottomKSketch("Name", 176864, numSamples);
        MinKSet mks = bks.create(pair.first);
        int numBoundaries = 19;
        List<String> boundaries = mks.getBoundaries(numBoundaries);
        List<Integer> ranks = TestTables.getRanks(boundaries, randomString);
        double maxErr = 0;
        double bound = 1.0 / Math.sqrt(numSamples);
        for(int i= 0; i < numBoundaries; i++) {
            double err = Math.abs(ranks.get(i) / ((float) suppSize) - (i + 1) / (numBoundaries + 1.0));
            if (err > maxErr)
                maxErr = err;
        }
        Assert.assertTrue(maxErr < bound);
    }

    @Test
    public void testStringTwoTables() {
        int suppSize = 100000;
        int length = 6;
        List<String> randomString = TestTables.randStringArray(suppSize, length);
        List<String> Part1 = new ArrayList<>();
        List<String> Part2 = new ArrayList<>();
        for (int i= 0; i < suppSize; i++) {
            if (i % 2 == 0)
                Part2.add(randomString.get(i));
            else
                Part1.add(randomString.get(i));
        }
        int num = suppSize*((int) Math.ceil(Math.log(suppSize)));
        Pair<Table, SortedMap<String, Integer>> pair1 = TestTables.randStringTable(num, Part1);
        Pair<Table, SortedMap<String, Integer>> pair2 = TestTables.randStringTable(num, Part2);
        int numSamples = 10000;
        BottomKSketch bks = new BottomKSketch("Name", 17864, numSamples);
        MinKSet mks1 = bks.create(pair1.first);
        MinKSet mks2 = bks.create(pair2.first);
        MinKSet mks3 = bks.add(mks1, mks2);
        int numBoundaries = 49;
        List<String> boundaries = mks3.getBoundaries(numBoundaries);
        List<Integer> ranks = TestTables.getRanks(boundaries, randomString);
        double maxErr = 0;
        double bound = 1.0 / Math.sqrt(numSamples);
        for(int i= 0; i < numBoundaries; i++) {
            double err = Math.abs(ranks.get(i) / ((float) suppSize) - (i + 1) / (numBoundaries + 1.0));
            if (err > maxErr)
                maxErr = err;
        }
        Assert.assertTrue(maxErr < bound);
    }


    @Test
    public void testParallelTable() {
        int suppSize = 100000;
        int length = 6;
        List<String> randomString = TestTables.randStringArray(suppSize, length);
        int num = suppSize*((int) Math.ceil(Math.log(suppSize)));
        Pair<Table, SortedMap<String, Integer>> pair = TestTables.randStringTable(num, randomString);
        Table t = pair.first;
        final int parts = 4;
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        for (int i = 0; i < parts; i++) {
            LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
            fragments.add(data);
        }
        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments);
        int numSamples = 10000;
        BottomKSketch bks = new BottomKSketch("Name", 1754, numSamples);
        MinKSet mks = big.blockingSketch(bks);
        int numBoundaries = 99;
        List<String> boundaries = mks.getBoundaries(numBoundaries);
        List<Integer> ranks = TestTables.getRanks(boundaries, randomString);
        double maxErr = 0;
        double bound = 1.0 / Math.sqrt(numSamples);
        for(int i= 0; i < numBoundaries; i++) {
            double err = Math.abs(ranks.get(i) / ((float) suppSize) - (i + 1) / (numBoundaries + 1.0));
            if (err > maxErr)
                maxErr = err;
        }
        Assert.assertTrue(maxErr < bound);
    }
}



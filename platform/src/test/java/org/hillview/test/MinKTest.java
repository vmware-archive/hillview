package org.hillview.test;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.SampleDistinctElementsSketch;
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
    private Boolean printOn = false;

    private double getMaxErr(int suppSize, int numBuckets, List<Integer> ranks) {
        double maxErr = 0;
        Assert.assertEquals((int) ranks.get(0), 0);
        Assert.assertEquals((int) ranks.get(numBuckets), suppSize -1);
        if (printOn) {
            System.out.printf("Min rank: %d\n", ranks.get(0));
            System.out.printf("Max rank: %d\n", ranks.get(numBuckets));
        }
        for (int i = 1; i < numBuckets; i++) {
            double err = Math.abs((ranks.get(i)/(suppSize - 1.0)) - i/((float) numBuckets));
            if (printOn)
                System.out.printf("%d has Rank %f, Error %f\n", i,
                        (ranks.get(i)/((float) suppSize -1.0)), err);
            if (err > maxErr)
                maxErr = err;
        }
        return maxErr;
    }

    private void printBoundaries(List<String> boundaries, List<Integer> ranks) {
        if(!printOn)
            return;
        int trueBuckets = boundaries.size() -1;
        System.out.printf("Total of %d buckets\n", trueBuckets);
        for (int i = 0; i <= trueBuckets; i++) {
            System.out.printf("%s, %d \n", boundaries.get(i), ranks.get(i));
        }
    }

    private double getErrBound(int numSamples) {
        double bound = 5.0 / Math.sqrt(numSamples);
        if (printOn)
            System.out.printf("Error bound: %f\n", bound);
        return bound;
    }


    private void testStringTable(int suppSize) {
        int length = 6;
        List<String> randomString = TestTables.randStringList(suppSize, length);
        int num = Math.max(10*suppSize, suppSize*((int) Math.ceil(Math.log(suppSize))));
        Pair<Table, SortedMap<String, Integer>> pair = TestTables.randStringTable(num, randomString);
        int numSamples = 10000;
        SampleDistinctElementsSketch bks = new SampleDistinctElementsSketch("Name", 176864, numSamples);
        assert pair.first != null;
        MinKSet<String> mks = bks.create(pair.first);
        if (printOn)
            System.out.printf("Table size: %d, non-null %d\n", num, mks.numPresent);
        int maxBuckets = 100;
        List<String> boundaries = mks.getBoundaries(maxBuckets);
        int numBuckets= boundaries.size() - 1;
        List<Integer> ranks = TestTables.getRanks(boundaries, randomString);
        this.printBoundaries(boundaries, ranks);
        double maxErr = this.getMaxErr(suppSize, numBuckets, ranks);
        double bound = this.getErrBound(numSamples);
        Assert.assertTrue(maxErr < bound);
    }

    @Test
    public void testSupportValues() {
        testStringTable(1);
        testStringTable(2);
        testStringTable(10);
        testStringTable(100);
        testStringTable(10000);
    }

    @Test
    public void testStringTwoTables() {
        int suppSize = 100000;
        int length = 6;
        List<String> randomString = TestTables.randStringList(suppSize, length);
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
        int numSamples = 5000;
        SampleDistinctElementsSketch bks = new SampleDistinctElementsSketch("Name", 17864, numSamples);
        assert pair1.first != null;
        assert pair2.first != null;
        MinKSet<String> mks1 = bks.create(pair1.first);
        MinKSet<String> mks2 = bks.create(pair2.first);
        MinKSet<String> mks3 = bks.add(mks1, mks2);
        assert mks3 != null;
        int maxBuckets = 50;
        List<String> boundaries = mks3.getBoundaries(maxBuckets);
        int numBuckets= boundaries.size() - 1;
        List<Integer> ranks = TestTables.getRanks(boundaries, randomString);
        double maxErr = this.getMaxErr(suppSize, numBuckets, ranks);
        double bound = this.getErrBound(numSamples);
        Assert.assertTrue(maxErr < bound);
    }


    @Test
    public void testParallelTable() {
        int suppSize = 2;
        int length = 6;
        List<String> randomString = TestTables.randStringList(suppSize, length);
        int num = Math.max(10*suppSize, suppSize*((int) Math.ceil(Math.log(suppSize))));
        Pair<Table, SortedMap<String, Integer>> pair = TestTables.randStringTable(num, randomString);
        Table t = pair.first;
        assert t != null;

        final int parts = 4;
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        for (int i = 0; i < parts; i++) {
            LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
            fragments.add(data);
        }
        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments);
        int numSamples = 10000;
        SampleDistinctElementsSketch bks = new SampleDistinctElementsSketch("Name", 1754, numSamples);
        MinKSet<String> mks = big.blockingSketch(bks);
        int maxBuckets = 100;
        List<String> boundaries = mks.getBoundaries(maxBuckets);
        int numBuckets= boundaries.size() - 1;
        List<Integer> ranks = TestTables.getRanks(boundaries, randomString);
        this.printBoundaries(boundaries, ranks);
        double maxErr = this.getMaxErr(suppSize, numBuckets, ranks);
        double bound = this.getErrBound(numSamples);
        Assert.assertTrue(maxErr < bound);
    }

}



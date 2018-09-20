package org.hillview.test;

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.*;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.TestTables;
import org.junit.Test;

public class CountSketchTest {
    public boolean toPrint = true;

    private void timeCreate(ExactCountSketch cs, ITable table) {
        long oldStart = System.nanoTime();
        FreqKList freqK = cs.create(table);
        long oldEnd = System.nanoTime();
        System.out.printf("Time: %d\n", oldEnd - oldStart);
        long [] estimate;
        for (Pair<RowSnapshot, Integer> pair: freqK.getSortedList()) {
            estimate = cs.result.getTrace(pair.first);
            if (toPrint)
                System.out.printf("%s: %d (%d)\n", pair.first.toString(), pair.second,
                        estimate[cs.result.csDesc.trials/2]);
        }
    }

    private void runCS(int buckets, int trials, double threshold, long seed, ITable table) {
        CountSketchDescription csDesc = new CountSketchDescription(buckets, trials, seed,
                table.getSchema());
        CountSketch cs = new CountSketch(csDesc);
        CountSketchResult result = cs.create(table);
        System.out.printf("Estimated 2 norm: %f, cutoff: %.2f\n", result.estimateNorm(),
            Math.ceil(threshold*result.estimateNorm()));
        ExactCountSketch exactCS = new ExactCountSketch(result, threshold);
        FreqKList freqK = exactCS.create(table);
        long [] estimate;
        for (Pair<RowSnapshot, Integer> pair: freqK.getSortedList()) {
        estimate = result.getTrace(pair.first);
        if (toPrint)
            System.out.printf("%s: %d (%d)\n", pair.first.toString(), pair.second,
                    estimate[trials/2]);
        }
    }

    @Test
    public void testPower() {
        int maxNum = 100;
        double exp = 2;
        ITable leftTable = TestTables.getPowerIntTable(maxNum, exp);
        int buckets = 50;
        int trials = 40;
        double threshold = 0.1;
        long seed = 385738753;
        runCS(buckets, trials, threshold, seed, leftTable);
    }

    @Test
    public void testCS2() {
        final int numCols = 1;
        final int size = 1000;
        final double base  = 1.1;
        final int range = 20;
        ITable table = TestTables.getHeavyIntTable(numCols, size, base, range);
        final int trials = 10;
        final int buckets= 100;
        long seed = 83839842;
        double threshold = 0.1;
        this.runCS(buckets, trials, threshold, seed, table);
    }


    @Test
    public void testZipf() {
        int range = 1000;
        double exp = 1.5;
        ITable leftTable = TestTables.getZipfTable(range, exp);
        int buckets = 200;
        int trials = 40;
        long seed = 387634753;
        double threshold = 0.01;
        this.runCS(buckets, trials, threshold, seed, leftTable);
    }

    @Test
    public void TestRepTable() {
        final int size = 10000;
        final int numCols = 1;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        Table rightTable = TestTables.getRepIntTable(size, numCols);
        int buckets = 200;
        int trials = 40;
        CountSketchDescription csDesc = new CountSketchDescription(buckets, trials, 3949495, leftTable.getSchema());
        CountSketch cs = new CountSketch(csDesc);
        CountSketchResult left = cs.create(leftTable);
        CountSketchResult right = cs.create(rightTable);
        CountSketchResult sum = cs.add(left, right);
        double threshold = 0.05;
        if(toPrint)
            System.out.printf("Estimated 2 norm: %f, cutoff: %.2f\n", sum.estimateNorm(),
                Math.ceil(threshold*sum.estimateNorm()));
        ExactCountSketch exactCS = new ExactCountSketch(sum, threshold);
        FreqKList freqKLeft = exactCS.create(leftTable);
        FreqKList freqKRight = exactCS.create(rightTable);
        FreqKList freqKSum = exactCS.add(freqKLeft, freqKRight);
        System.out.printf("%d, %d, %d\n", freqKLeft.getSize(), freqKRight.getSize(), freqKSum.getSize());
        long [] estimate;
        for (Pair<RowSnapshot, Integer> pair: freqKSum.getSortedList()) {
            estimate = sum.getTrace(pair.first);
            if (toPrint)
                System.out.printf("%s: %d (%d)\n", pair.first.toString(), pair.second,
                        estimate[trials/2]);
        }
    }
}

package org.hiero.sketch;

import org.hiero.dataset.ParallelDataSet;
import org.hiero.sketches.*;
import org.hiero.table.FullMembership;
import org.hiero.table.IntArrayColumn;
import org.hiero.table.SmallTable;
import org.hiero.table.api.ITable;
import org.hiero.utils.IntArrayGenerator;
import org.hiero.utils.Randomness;
import org.hiero.utils.TestTables;
import org.junit.Test;

public class HLLTest {
    @Test
    public void testHLL() {
        final int size = 500000;
        final int range = 50;
        final IntArrayColumn col = IntArrayGenerator.getRandIntArray(size, range, "Test");
        final Randomness rn = new Randomness();
        final int accuracy = 16;
        final int seed = rn.nextInt();
        final HLogLog hll = new HLogLog(accuracy, seed);
        final FullMembership memSet = new FullMembership(size);
        hll.createHLL(col, memSet);
    }

    @Test
    public void testHLLSketch() {
        final int numCols = 1;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 1;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final HLogLog hll = all.blockingSketch(new HLogLogSketch(colName,12,1234567));
    }
}

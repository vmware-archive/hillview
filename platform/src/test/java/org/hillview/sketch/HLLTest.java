package org.hillview.sketch;

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.table.FullMembership;
import org.hillview.table.IntArrayColumn;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.utils.IntArrayGenerator;
import org.hillview.utils.Randomness;
import org.hillview.utils.TestTables;
import org.junit.Test;

import static org.junit.Assert.*;


public class HLLTest {
    @Test
    public void testHLL() {
        final int size = 2000000;
        final int range = 20000;
        final Randomness rn = new Randomness();
        final IntArrayColumn col = IntArrayGenerator.getRandIntArray(size, range, "Test", rn);
        final int accuracy = 14;
        final long seed = 0; //deterministic seed for testing
        final HLogLog hll = new HLogLog(accuracy, seed);
        final FullMembership memSet = new FullMembership(size);
        hll.createHLL(col, memSet);
        long result = hll.distinctItemsEstimator();
        assertTrue((result > (0.9 * range)) && (result < (1.1 * range)));
    }

    @Test
    public void testHLLSketch() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols); //range is 5 * bigSize
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final HLogLog hll = all.blockingSketch(new HLogLogSketch(colName,16,12345678));
        assertTrue(hll.distinctItemsEstimator() > 85000);
    }
}

package org.hiero.sketch;

import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.SemiExplicitConverter;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;


public class DistinctStringSketchTest {

    @Test
    public void DistinctSketchTest() {
        final int tableSize = 1000;
        final Table myTable = TestUtil.createTable(tableSize);
        final DistinctStringsSketch mySketch = new DistinctStringsSketch(20, "Name");
        DistinctStrings result = mySketch.create(myTable);
        int size = result.size();
        Assert.assertTrue(size <= 10);
        SemiExplicitConverter converter = result.getStringConverter();
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(1, size + 1, size);
        Hist1DSketch histSketch = new Hist1DSketch(desc, "Name", converter);
        Histogram1D hist = histSketch.create(myTable);
    }
    @Test
    public void DistinctSketchTest2() {
        final int tableSize = 1000;
        final SmallTable myTable = TestUtil.createSmallTable(tableSize);
        final ParallelDataSet<ITable> all = TableTest.makeParallel(myTable, tableSize/10);
        final DistinctStrings ds = all.blockingSketch(new DistinctStringsSketch(tableSize, "Name"));
        SemiExplicitConverter converter = ds.getStringConverter();
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(-1, ds.size(), ds.size() + 1);
        Histogram1D hist = all.blockingSketch(new Hist1DSketch(desc, "Name", converter));
    }
}

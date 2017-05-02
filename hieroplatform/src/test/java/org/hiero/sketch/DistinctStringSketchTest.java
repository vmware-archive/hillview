package org.hiero.sketch;

import org.hiero.dataset.ParallelDataSet;
import org.hiero.sketches.*;
import org.hiero.utils.TestTables;
import org.hiero.table.SemiExplicitConverter;
import org.hiero.table.SmallTable;
import org.hiero.table.Table;
import org.hiero.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

public class DistinctStringSketchTest {
    SemiExplicitConverter getStringConverter(DistinctStrings ds) {
        SemiExplicitConverter converter = new SemiExplicitConverter();
        int i = 0;
        for (String item : ds.getStrings()) {
            converter.set(item, i);
            i++;
        }
        return converter;
    }

    @Test
    public void DistinctSketchTest() {
        final int tableSize = 1000;
        final Table myTable = TestUtil.createTable(tableSize);
        final DistinctStringsSketch mySketch = new DistinctStringsSketch(20, "Name");
        DistinctStrings result = mySketch.create(myTable);
        int size = result.size();
        Assert.assertTrue(size <= 10);
        SemiExplicitConverter converter = getStringConverter(result);
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(1, size + 1, size);
        Hist1DSketch histSketch = new Hist1DSketch(desc, "Name", converter);
        Histogram1D hist = histSketch.create(myTable);
    }
    @Test
    public void DistinctSketchTest2() {
        final int tableSize = 1000;
        final SmallTable myTable = TestUtil.createSmallTable(tableSize);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(myTable, tableSize/10);
        final DistinctStrings ds = all.blockingSketch(new DistinctStringsSketch(tableSize, "Name"));
        SemiExplicitConverter converter = getStringConverter(ds);
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(-1, ds.size(), ds.size() + 1);
        Histogram1D hist = all.blockingSketch(new Hist1DSketch(desc, "Name", converter));
    }
}
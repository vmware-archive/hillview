package org.hillview.sketch;

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.utils.TestTables;
import org.hillview.table.SemiExplicitConverter;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

public class DistinctStringSketchTest {
    private SemiExplicitConverter getStringConverter(DistinctStrings ds) {
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
        HistogramSketch histSketch = new HistogramSketch(desc, "Name", converter);
        Histogram hist = histSketch.create(myTable);
    }

    @Test
    public void DistinctSketchTest2() {
        final int tableSize = 1000;
        final SmallTable myTable = TestUtil.createSmallTable(tableSize);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(myTable, tableSize/10);
        final DistinctStrings ds = all.blockingSketch(new DistinctStringsSketch(tableSize, "Name"));
        SemiExplicitConverter converter = getStringConverter(ds);
        BucketsDescriptionEqSize desc = new BucketsDescriptionEqSize(-1, ds.size(), ds.size() + 1);
        Histogram hist = all.blockingSketch(new HistogramSketch(desc, "Name", converter));
    }
}

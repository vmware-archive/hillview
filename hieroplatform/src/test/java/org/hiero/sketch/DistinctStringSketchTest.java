package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.SemiExplicitConverter;
import org.hiero.sketch.table.Table;
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
}

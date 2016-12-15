package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.BucketsDescriptionEqSize;
import org.hiero.sketch.spreadsheet.Histogram1D;
import org.hiero.sketch.table.DoubleArrayColumn;
import org.hiero.sketch.table.FullMembership;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;


public class Histogram1DTest {
    @Test
    public void testHistogram1D() throws Exception {
        final int bucketnum = 110;
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0,100,bucketnum);
        Histogram1D hist = new Histogram1D(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(10000);
        FullMembership fmap = new FullMembership(10000);
        hist.createHistogram(col, fmap, null);
        int size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist.getBucket(i).getCount();
        assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), 10000);
        Histogram1D hist1 = new Histogram1D(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap1 = new FullMembership(20000);
        hist1.createHistogram(col1, fmap1, null);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist1.getBucket(i).getCount();
        assertEquals(size + hist1.getMissingData() + hist1.getOutOfRange(), 20000);
        Histogram1D hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist2.getBucket(i).getCount();
        assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 30000);
    }
}

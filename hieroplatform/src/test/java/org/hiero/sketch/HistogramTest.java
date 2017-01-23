package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.DoubleArrayColumn;
import org.hiero.sketch.table.FullMembership;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;


public class HistogramTest {
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
        Histogram1D hist3 = new Histogram1D(buckDes);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap2 = new FullMembership(20000);
        hist3.createSampleHistogram(col2, fmap2, null, 0.1);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist3.getBucket(i).getCount();
        assertEquals(size + hist3.getMissingData() + hist3.getOutOfRange(), 2000);
        }

    @Test
    public void testHistogram1DLight() throws Exception {
        final int bucketnum = 110;
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0,100,bucketnum);
        Histogram1DLight hist = new Histogram1DLight(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(10000);
        FullMembership fmap = new FullMembership(10000);
        hist.createHistogram(col, fmap, null);
        int size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist.getBucket(i);
        assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), 10000);
        Histogram1DLight hist1 = new Histogram1DLight(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap1 = new FullMembership(20000);
        hist1.createHistogram(col1, fmap1, null);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist1.getBucket(i);
        assertEquals(size + hist1.getMissingData() + hist1.getOutOfRange(), 20000);
        Histogram1DLight hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist2.getBucket(i);
        assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 30000);
        Histogram1DLight hist3 = new Histogram1DLight(buckDes);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap2 = new FullMembership(20000);
        hist3.createSampleHistogram(col2, fmap2, null, 0.1);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist3.getBucket(i);
        assertEquals(size + hist3.getMissingData() + hist3.getOutOfRange(), 2000);
    }

    @Test
    public void testHistogram2DHeavy() throws Exception {
        final int bucketnum = 110;
        BucketsDescriptionEqSize buckDes1 = new BucketsDescriptionEqSize(0,100,bucketnum);
        BucketsDescriptionEqSize buckDes2 = new BucketsDescriptionEqSize(0,100,bucketnum);
        Histogram2DHeavy hist = new Histogram2DHeavy(buckDes1, buckDes2);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(10000);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(10000);
        FullMembership fmap = new FullMembership(10000);
        hist.createHistogram(col1, col2, null, null, fmap);
        int size = 0;
        Histogram2DHeavy hist1 = new Histogram2DHeavy(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(20000);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap1 = new FullMembership(20000);
        hist1.createHistogram(col3, col4, null, null, fmap1);
        size = 0;
        int size1 = 0;
        int size2 = 0;
        for (int i = 0; i < bucketnum; i++)
            for (int j = 0; j< bucketnum; j++)
                size += hist1.getBucket(i,j).getCount();
        size += hist1.getMissingData();
        size += hist1.getOutOfRange();
        for (int i = 0; i < hist1.getNumOfBucketsD1(); i++)
            size1 += hist1.getMissingHistogramD1().getBucket(i).getCount();
        size1 += hist1.getMissingHistogramD1().getOutOfRange();
        for (int i = 0; i < hist1.getNumOfBucketsD2(); i++)
            size2 += hist1.getMissingHistogramD2().getBucket(i).getCount();
        size2 += hist1.getMissingHistogramD2().getOutOfRange();

        assertEquals(size + size1 + size2, 20000);
        Histogram2DHeavy hist2 = hist1.union(hist);
        size = 0;
        size1 = 0;
        size2 =0;
        for (int i = 0; i < bucketnum; i++)
            for (int j = 0; j< bucketnum; j++)
                size += hist2.getBucket(i,j).getCount();
        size += hist2.getMissingData();
        size += hist2.getOutOfRange();
        for (int i = 0; i < hist2.getNumOfBucketsD1(); i++)
            size1 += hist2.getMissingHistogramD1().getBucket(i).getCount();
        size1 += hist2.getMissingHistogramD1().getOutOfRange();
        for (int i = 0; i < hist2.getNumOfBucketsD2(); i++)
            size2 += hist2.getMissingHistogramD2().getBucket(i).getCount();
        size2 += hist2.getMissingHistogramD2().getOutOfRange();

        assertEquals(size + size1 + size2, 30000);
        Histogram2DHeavy hist3 = new Histogram2DHeavy(buckDes1, buckDes2);
        DoubleArrayColumn col5 = DoubleArrayTest.generateDoubleArray(20000);
        DoubleArrayColumn col6 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap2 = new FullMembership(20000);
        hist3.createSampleHistogram(col5, col6, null, null, fmap2, 0.1);
    }

    @Test
    public void testHeatMap() {
        final int bucketnum = 110;
        BucketsDescriptionEqSize buckDes1 = new BucketsDescriptionEqSize(0,100,bucketnum);
        BucketsDescriptionEqSize buckDes2 = new BucketsDescriptionEqSize(0,100,bucketnum);
        HeatMap hm = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col1 = this.generateDoubleArray(10000, 5);
        DoubleArrayColumn col2 = this.generateDoubleArray(10000, 3);
        FullMembership fmap = new FullMembership(10000);
        hm.createHistogram(col1, col2, null, null, fmap);
        int size0 = 0;
        int size1 = 0;
        int size2 = 0;
        for (int i = 0; i < hm.getNumOfBucketsD1(); i++)
            for (int j = 0; j < hm.getNumOfBucketsD2(); j++)
                size0 += hm.getBucket(i,j);
        size0 += hm.getMissingData();
        size0 += hm.getOutOfRange();
        for (int i = 0; i < hm.getNumOfBucketsD1(); i++)
            size1 += hm.getMissingHistogramD1().getBucket(i);
        size1 += hm.getMissingHistogramD1().getOutOfRange();
        for (int i = 0; i < hm.getNumOfBucketsD2(); i++)
            size2 += hm.getMissingHistogramD2().getBucket(i);
        size2 += hm.getMissingHistogramD2().getOutOfRange();
        assertEquals(10000, size0 + size1 + size2);

        HeatMap hm1 = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(20000);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(20000);
        FullMembership fmap1 = new FullMembership(20000);
        hm1.createSampleHistogram(col3, col4, null, null, fmap1, 0.1);
        HeatMap hm2 = hm.union(hm1);
        assertEquals(hm.getSize() + hm1.getSize(), hm2.getSize());
    }

    private final ColumnDescription desc = new ColumnDescription("SQRT", ContentsKind.Double, true);

    /**
     * Generates a double array with every fifth entry missing
     */
    public  DoubleArrayColumn generateDoubleArray(final int size, final int skip) {
        final DoubleArrayColumn col = new DoubleArrayColumn(desc, size);
        for (int i = 0; i < size; i++) {
            col.set(i, Math.sqrt(i + 1));
            if ((i % skip) == 0)
                col.setMissing(i);
        }
        return col;
    }
}

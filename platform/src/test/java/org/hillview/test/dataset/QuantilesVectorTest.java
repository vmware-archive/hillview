package org.hillview.test.dataset;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.QuantilesVectorSketch;
import org.hillview.sketches.results.*;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class QuantilesVectorTest extends BaseTest {
    @Test
    public void sketchTest() {
        ITable table = TestTables.testTable();
        IDataSet<ITable> local = new LocalDataSet<ITable>(table);
        String[] boundaries = new String[] { "A", "M" };
        IHistogramBuckets buckets = new StringHistogramBuckets(boundaries, "Z");
        double[] samplingRates = new double[] { 1.0, 0.5 };
        QuantilesVectorSketch sk = new QuantilesVectorSketch(buckets, "Name", "Age", samplingRates, 1);
        QuantilesVector qv = local.blockingSketch(sk);
        Assert.assertNotNull(qv);
        Assert.assertEquals(2, qv.data.length);
        Assert.assertEquals(1.0, qv.data[0].min, .01);
        Assert.assertEquals(30.0, qv.data[0].max, .01);
        Assert.assertEquals(8, qv.data[0].size());
        Assert.assertEquals(3.0, qv.data[1].min, .01);
        Assert.assertEquals(20.0, qv.data[1].max, .01);
    }

    @Test
    public void histogramAndQuantilesTest() {
        ITable table = TestTables.testTable();
        IDataSet<ITable> local = new LocalDataSet<ITable>(table);
        String[] boundaries = new String[] { "A", "M" };
        IHistogramBuckets buckets = new StringHistogramBuckets(boundaries, "Z");
        HistogramSketch hissk = new HistogramSketch(buckets, "Name", 1.0, 0, null);
        Histogram h = local.blockingSketch(hissk);
        Assert.assertNotNull(h);
        final int desiredSamples = 10;
        double[] samplingRates = new double[h.getBucketCount()];
        for (int i = 0; i < h.getBucketCount(); i++) {
            long count = h.buckets[i];
            double rate = (double)(desiredSamples * desiredSamples) / count;
            if (rate > 1.0)
                rate = 1;
            samplingRates[i] = rate;
        }
        QuantilesVectorSketch sk = new QuantilesVectorSketch(buckets, "Name", "Age", samplingRates, 1);
        QuantilesVector qv = local.blockingSketch(sk);
        Assert.assertNotNull(qv);
        qv = qv.quantiles(desiredSamples);
        for (NumericSamples n: qv.data) {
            Assert.assertTrue(n.size() <= desiredSamples);
            Assert.assertTrue(n.size() > 0);
        }
    }
}

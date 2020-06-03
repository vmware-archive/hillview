package org.hillview.test.dataset;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.Groups;
import org.hillview.sketches.HistogramQuantilesSketch;
import org.hillview.sketches.HistogramSketch;
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
        IHistogramBuckets buckets = new StringHistogramBuckets("Name", boundaries, "Z");
        HistogramQuantilesSketch sk = new HistogramQuantilesSketch(
                "Age", 8, 0, buckets);
        Groups<SampleSet> qv = local.blockingSketch(sk);
        Assert.assertNotNull(qv);
        Assert.assertEquals(2, qv.perBucket.size());
        Assert.assertEquals(1.0, qv.perBucket.get(0).min, .01);
        Assert.assertEquals(30.0, qv.perBucket.get(0).max, .01);
        Assert.assertEquals(8, qv.perBucket.get(0).size());
        Assert.assertEquals(3.0, qv.perBucket.get(1).min, .01);
        Assert.assertEquals(20.0, qv.perBucket.get(1).max, .01);
    }

    @Test
    public void histogramAndQuantilesTest() {
        ITable table = TestTables.testTable();
        IDataSet<ITable> local = new LocalDataSet<ITable>(table);
        String[] boundaries = new String[] { "A", "M" };
        IHistogramBuckets buckets = new StringHistogramBuckets("Name", boundaries, "Z");
        HistogramSketch hissk = new HistogramSketch(buckets, 1.0, 0, null);
        Histogram h = local.blockingSketch(hissk);
        Assert.assertNotNull(h);
        final int desiredSamples = 10;
        HistogramQuantilesSketch sk = new HistogramQuantilesSketch(
                "Age", desiredSamples, 1, buckets);
        Groups<SampleSet> qv = local.blockingSketch(sk);
        Assert.assertNotNull(qv);
        for (SampleSet n: qv.perBucket) {
            SampleSet n0 = n.quantiles(desiredSamples);
            Assert.assertTrue(n0.size() <= desiredSamples);
            Assert.assertTrue(n0.size() > 0);
        }
    }
}

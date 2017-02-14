package org.hiero.sketch;
import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import static org.hiero.sketch.TableTest.SplitTable;
import static org.hiero.sketch.TableTest.getIntTable;
import static org.hiero.sketch.TableTest.getRepIntTable;
import static org.junit.Assert.assertEquals;

/**
 * Test class for the sketches of all types of histograms.
 * IntConverter is used for the IStringConverter due to a reported issue.
 * Will be replaced by Null once it is resolved, allowing the default converter to kisk in.
 */
public class HistSketchTest {
    @Test
    public void Hist1DLightTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = getRepIntTable(tableSize, numCols);
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final Hist1DLightSketch mySketch = new Hist1DLightSketch(buckets,
                myTable.getSchema().
                        getColumnNames().
                               iterator().next(), new IntConverter());
        Histogram1DLight result = mySketch.getHistogram(myTable);
        int size = 0;
        int bucketnum = result.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += result.getCount(i);
        assertEquals(size + result.getMissingData() + result.getOutOfRange(),
                myTable.getMembershipSet().getSize());
    }

    @Test
    public void Hist1DLightTest2() {
        final int numCols = 1;
        final int maxSize = 50;
        final int bigSize = 100000;
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final List<SmallTable> tabList = SplitTable(bigTable, bigSize/10);
        final ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        final ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        final Histogram1DLight hdl = all.blockingSketch(new Hist1DLightSketch(buckets, colName,
                new IntConverter(), 0.5));
        int size = 0;
        int bucketnum = hdl.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += hdl.getCount(i);
        assertEquals(size + hdl.getMissingData() + hdl.getOutOfRange(), (int)
                (bigTable.getMembershipSet().getSize() * 0.5));
    }

    @Test
    public void Hist1DTest2() {
        final int numCols = 1;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.1;
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final List<SmallTable> tabList = SplitTable(bigTable, bigSize/10);
        final ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        final ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        final Histogram1D hd = all.blockingSketch(new Hist1DSketch(buckets, colName,
                new IntConverter(), rate));
        int size = 0;
        int bucketnum = hd.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += hd.getBucket(i).getCount();
        assertEquals(size + hd.getMissingData() + hd.getOutOfRange(), (int)
                (bigTable.getMembershipSet().getSize() * rate));
    }

    @Test
    public void HeatMapSketchTest() {
        final int numCols = 2;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.5;
        final BucketsDescriptionEqSize buckets1 = new BucketsDescriptionEqSize(1, 50, 10);
        final BucketsDescriptionEqSize buckets2 = new BucketsDescriptionEqSize(1, 50, 15);
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final Iterator<String> iter = bigTable.getSchema().getColumnNames().iterator();
        final String colName1 = iter.next();
        final String colName2 = iter.next();
        final List<SmallTable> tabList = SplitTable(bigTable, bigSize/10);
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        final ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        final HeatMap hm = all.blockingSketch(new HeatMapSketch(buckets1, buckets2,
                                                            new IntConverter(), new IntConverter(),
                                                            colName1, colName2, rate));
        HistogramTest.basicTestHeatMap(hm, (long) (bigSize * rate));
    }

    @Test
    public void Hist2DSketchTest() {
        final int numCols = 2;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.5;
        final BucketsDescriptionEqSize buckets1 = new BucketsDescriptionEqSize(1, 50, 10);
        final BucketsDescriptionEqSize buckets2 = new BucketsDescriptionEqSize(1, 50, 15);
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final Iterator<String> iter = bigTable.getSchema().getColumnNames().iterator();
        final String colName1 = iter.next();
        final String colName2 = iter.next();
        final List<SmallTable> tabList = SplitTable(bigTable, bigSize/10);
        final ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        final ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        final Histogram2DHeavy hm = all.blockingSketch(new Hist2DSketch(buckets1, buckets2,
                new IntConverter(), new IntConverter(),
                colName1, colName2, rate));
        HistogramTest.basicTest2DHeavy(hm, (long) (bigSize * rate));
    }
}






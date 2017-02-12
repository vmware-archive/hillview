package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.hiero.sketch.table.api.IndexComparator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.SplitTable;
import static org.hiero.sketch.TableTest.getIntTable;
import static org.hiero.sketch.TableTest.getRepIntTable;
import static org.junit.Assert.assertEquals;

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
        String colName = bigTable.getSchema().getColumnNames().iterator().next();
        List<SmallTable> tabList = SplitTable(bigTable, 10000);
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        Histogram1DLight hdl = all.blockingSketch(new Hist1DLightSketch(buckets, colName,
                new IntConverter(), 0.5));
        int size = 0;
        int bucketnum = hdl.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += hdl.getCount(i);
        assertEquals(size + hdl.getMissingData() + hdl.getOutOfRange(), (int)
                (bigTable.getMembershipSet().getSize() * 0.5));
    }
}


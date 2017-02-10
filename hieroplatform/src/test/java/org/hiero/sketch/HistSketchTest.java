package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.BucketsDescriptionEqSize;
import org.hiero.sketch.spreadsheet.Hist1DLightSketch;
import org.hiero.sketch.spreadsheet.Histogram1DLight;
import org.hiero.sketch.table.Table;
import org.junit.Test;

import static org.hiero.sketch.TableTest.getRepIntTable;
import static org.junit.Assert.assertEquals;

public class HistSketchTest {
    @Test
    public void Hist1DLightTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = getRepIntTable(tableSize, numCols);
        BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final Hist1DLightSketch mySketch = new Hist1DLightSketch(buckets,
                myTable.getSchema().
                        getColumnNames().
                               iterator().next(), null);
        Histogram1DLight result = mySketch.getHistogram(myTable);
        int size = 0;
        int bucketnum = result.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += result.getCount(i);
        assertEquals(size + result.getMissingData() + result.getOutOfRange(),
                myTable.getMembershipSet().getSize());
    }
}


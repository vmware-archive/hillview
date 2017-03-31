package org.hiero.sketch;

import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.utils.TestTables;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

public class BasicStatSketchTest {
    @Test
    public void StatSketchTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        final BasicColStatSketch mySketch = new BasicColStatSketch(
                myTable.getSchema().getColumnNames().iterator().next(), null, 0 , 0.1);
        BasicColStats result = mySketch.create(myTable);
        Assert.assertEquals(result.getRowCount(), 100);
    }

    @Test
    public void StatSketchTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();

        IDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final BasicColStats result = all.blockingSketch(new BasicColStatSketch(colName, null));
        final BasicColStatSketch mySketch = new BasicColStatSketch(
                bigTable.getSchema().getColumnNames().iterator().next(), null);
        BasicColStats result1 = mySketch.create(bigTable);
        Assert.assertEquals(result.getMoment(1), result1.getMoment(1), 0.001);
    }
}

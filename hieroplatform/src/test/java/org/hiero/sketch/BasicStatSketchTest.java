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
import java.util.List;
import static org.hiero.sketch.TableTest.SplitTable;
import static org.hiero.sketch.TableTest.getIntTable;
import static org.hiero.sketch.TableTest.getRepIntTable;
import static org.junit.Assert.assertEquals;

public class BasicStatSketchTest {
    @Test
    public void StatSketchTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = getRepIntTable(tableSize, numCols);

        final BasicColStatSketch mySketch = new BasicColStatSketch(
                myTable.getSchema().getColumnNames().iterator().next(), null, 0 , 0.1);
        BasicColStat result = mySketch.create(myTable);
        assertEquals(result.getSize(), 100);
    }

    @Test
    public void StatSketchTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final List<SmallTable> tabList = SplitTable(bigTable, bigSize/10);
        final ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        final ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        final BasicColStat result = all.blockingSketch(new BasicColStatSketch(colName, null));
        final BasicColStatSketch mySketch = new BasicColStatSketch(
                bigTable.getSchema().getColumnNames().iterator().next(), null);
        BasicColStat result1 = mySketch.create(bigTable);
        assertEquals(result.getMoment(1), result1.getMoment(1), 0.001);
    }
}

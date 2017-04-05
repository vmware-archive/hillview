package org.hiero.sketch;

import org.hiero.dataset.LocalDataSet;
import org.hiero.dataset.ParallelDataSet;
import org.hiero.dataset.api.IDataSet;
import org.hiero.sketches.FreqKList;
import org.hiero.sketches.FreqKSketch;
import org.hiero.table.HashSubSchema;
import org.hiero.table.SmallTable;
import org.hiero.table.Table;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;
import org.hiero.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FreqKTest {
    @Test
    public void testTopK1() {
        final int numCols = 2;
        final int maxSize = 10;
        final int size = 100;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), maxSize);
    //    System.out.println(fk.create(leftTable).toString());
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final int maxSize = 10;
        final int size = 1000;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        Table rightTable = TestTables.getRepIntTable(size, numCols);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), maxSize);
        fk.add(fk.create(leftTable), fk.create(rightTable));
    }

    @Test
    public void testTopK3() {
        final int numCols = 2;
        final int maxSize = 25;
        final double base = 2.0;
        final int range = 14;
        final int size = 20000;
        SmallTable leftTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), maxSize);
    }

    @Test
    public void testTopK4() {
        final int numCols = 2;
        final int maxSize = 25;
        final double base = 2.0;
        final int range = 14;
        final int size = 20000;
        SmallTable leftTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        SmallTable rightTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), maxSize);
        FreqKList freqKList = Converters.checkNull(fk.add(fk.create(leftTable),
                fk.create(rightTable)));
        Assert.assertNotNull(freqKList.toString());
    }

    @Test
    public void testTopK5() {
        final int numCols = 2;
        final int maxSize = 25;
        final double base = 2.0;
        final int range = 16;
        final int size = 100000;
        SmallTable bigTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        List<ITable> tabList = TestTables.splitTable(bigTable, 1000);
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        tabList.forEach(t -> a.add(new LocalDataSet<ITable>(t)));
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        FreqKSketch fk = new FreqKSketch(bigTable.getSchema(), maxSize);
        Assert.assertNotNull(all.blockingSketch(fk).toString());
    }

    @Test
    public void testTopK6() {
        Table t = TestTables.testRepTable();
        HashSubSchema hss = new HashSubSchema();
        hss.add("Age");
        FreqKSketch fk = new FreqKSketch(t.getSchema().project(hss), 5);
        String s = "10: (3-4)\n20: (3-4)\n30: (2-3)\n40: (1-2)\nError bound: 1\n";
        //Assert.assertEquals(fk.create(t).toString(), s);
    }
}

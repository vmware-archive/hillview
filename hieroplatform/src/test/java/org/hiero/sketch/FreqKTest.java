package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.FreqKList;
import org.hiero.sketch.spreadsheet.FreqKSketch;
import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.GetTable;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FreqKTest {
    @Test
    public void testTopK1() {
        final int numCols = 2;
        final int maxSize = 10;
        final int size = 100;
        Table leftTable = GetTable.getRepIntTable(size, numCols);
        List<ColumnDescription> cdl = leftTable.getSchema().getColumnNames().stream().map(colName ->
                leftTable.getSchema().getDescription(colName)).collect(Collectors.toList());
        FreqKSketch fk = new FreqKSketch(cdl, maxSize);
    //    System.out.println(fk.create(leftTable).toLongString());
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final int maxSize = 10;
        final int size = 1000;
        Table leftTable = GetTable.getRepIntTable(size, numCols);
        Table rightTable = GetTable.getRepIntTable(size, numCols);
        List<ColumnDescription> cdl = leftTable.getSchema().getColumnNames().stream().map(colName ->
                leftTable.getSchema().getDescription(colName)).collect(Collectors.toList());
        FreqKSketch fk = new FreqKSketch(cdl, maxSize);
    //  System.out.println(fk.add(fk.create(leftTable), fk.create(rightTable)).toLongString());
    }

    @Test
    public void testTopK3() {
        final int numCols = 2;
        final int maxSize = 25;
        final double base = 2.0;
        final int range = 14;
        final int size = 20000;
        SmallTable leftTable = GetTable.getHeavyIntTable(numCols, size, base, range);
        List<ColumnDescription> cdl = leftTable.getSchema().getColumnNames().stream().map(colName ->
                leftTable.getSchema().getDescription(colName)).collect(Collectors.toList());
        FreqKSketch fk = new FreqKSketch(cdl, maxSize);
        //System.out.println(fk.create(leftTable).toLongString());
    }

    @Test
    public void testTopK4() {
        final int numCols = 2;
        final int maxSize = 25;
        final double base = 2.0;
        final int range = 14;
        final int size = 20000;
        SmallTable leftTable = GetTable.getHeavyIntTable(numCols, size, base, range);
        SmallTable rightTable = GetTable.getHeavyIntTable(numCols, size, base, range);
        List<ColumnDescription> cdl = leftTable.getSchema().getColumnNames().stream().map(colName ->
                leftTable.getSchema().getDescription(colName)).collect(Collectors.toList());
        FreqKSketch fk = new FreqKSketch(cdl, maxSize);
        FreqKList freqKList = Converters.checkNull(fk.add(fk.create(leftTable), fk.create(rightTable)));
        freqKList.toLongString();
    }

    @Test
    public void testTopK5() {
        final int numCols = 2;
        final int maxSize = 25;
        final double base = 2.0;
        final int range = 16;
        final int size = 100000;
        SmallTable bigTable = GetTable.getHeavyIntTable(numCols, size, base, range);
        List<ITable> tabList = GetTable.splitTable(bigTable, 1000);
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        tabList.forEach(t -> a.add(new LocalDataSet<ITable>(t)));
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        List<ColumnDescription> cdl = bigTable.getSchema().getColumnNames().stream().map(colName ->
                bigTable.getSchema().getDescription(colName)).collect(Collectors.toList());
        FreqKSketch fk = new FreqKSketch(cdl, maxSize);
        System.out.println(all.blockingSketch(fk).toLongString());
    }

    @Test
    public void testTopK6() {
        Table t = GetTable.testRepTable();
        List<ColumnDescription> cdl = new ArrayList<>();
        cdl.add(t.getSchema().getDescription("Age"));
        FreqKSketch fk = new FreqKSketch(cdl, 5);
        System.out.println(fk.create(t).toLongString());
    }
}

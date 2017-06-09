package org.hillview.sketch;

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.table.RecordOrder;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.utils.RankInTable;
import org.hillview.utils.TestTables;
import org.junit.Test;

import java.util.Arrays;

public class SampleQuantileSketchTest {

    @Test
    public void SQSTest1() {
        final int numCols = 2;
        final int leftSize = 200000;
        final int rightSize =200000;
        final int resolution = 100;
        final SmallTable leftTable = TestTables.getIntTable(leftSize, numCols);
        RecordOrder rso = new RecordOrder();
        for (String colName : leftTable.getSchema().getColumnNames()) {
            rso.append(
                    new ColumnSortOrientation(leftTable.getSchema().getDescription(colName), true));
        }
        final SampleQuantileSketch sqSketch = new
                SampleQuantileSketch(rso, resolution, leftSize + rightSize);
        final SampleList leftQ = sqSketch.create(leftTable);
        RankInTable rLT = new RankInTable(leftTable, rso);
        System.out.println(Arrays.toString(rLT.getRank(leftQ.getQuantiles(9))));
        /*
        final SmallTable rightTable = TestTables.getIntTable(rightSize, numCols);
        final SampleList rightQ = sqSketch.create(rightTable);
        RankInTable rRT = new RankInTable(rightTable, rso);
        System.out.println(Arrays.toString(rRT.getRank(rightQ.getQuantiles(39))));
        final SampleList mergedQ = sqSketch.add(leftQ, rightQ);
        */
    }

    @Test
    public void SQSTest2() {
        final int numCols = 2;
        final int size = 500000;
        final int resolution = 100;
        final ITable Table = TestTables.getIntTable(size, numCols);
        RecordOrder rso = new RecordOrder();
        for (String colName : Table.getSchema().getColumnNames()) {
            rso.append(
                    new ColumnSortOrientation(Table.getSchema().getDescription(colName), true));
        }
        ParallelDataSet<ITable> all = TestTables.makeParallel(Table, 200000);
        SampleList sl = all.blockingSketch(new SampleQuantileSketch(rso, resolution, size));
        /*
        System.out.printf("Sample of size: %d", sl.table.getNumOfRows());
        for (int i =0; i < 10; i++)
            System.out.printf("Element of rank i: %s\n", sl.getRow(0.1*i).toString());*/
        RankInTable rIT = new RankInTable(Table, rso);
        System.out.println(Arrays.toString(rIT.getRank(sl.getQuantiles(19))));
    }
}
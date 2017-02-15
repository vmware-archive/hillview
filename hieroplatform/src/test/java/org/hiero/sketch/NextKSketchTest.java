package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.NextKList;
import org.hiero.sketch.spreadsheet.NextKSketch;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
import org.hiero.sketch.table.api.IndexComparator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.*;

public class NextKSketchTest {

    @Test
    public void testTopK1() {
        final int numCols = 2;
        final int maxSize = 50;
        final int rightSize = 1000;
        final int leftSize = 1000;
        final Table leftTable = getRepIntTable(leftSize, numCols);
        //System.out.println(leftTable.toLongString(50));
        RecordOrder cso = new RecordOrder();
        for (String colName : leftTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(leftTable.getSchema().getDescription(colName), true));
        }
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        //System.out.printf("Top Row %s. %n", topRow.toString());
        final NextKSketch nk = new NextKSketch(cso, topRow, maxSize);
        final NextKList leftK = nk.getNextKList(leftTable);
        IndexComparator leftComp = cso.getComparator(leftK.table);
        for (int i = 0; i < (leftK.table.getNumOfRows() - 1); i++)
            assertTrue(leftComp.compare(i, i + 1) <= 0);
        //System.out.println(leftK.toLongString(maxSize));

        final RowSnapshot topRow2 = new RowSnapshot(leftTable, 100);
        //System.out.printf("Top Row %s. %n", topRow2.toString());
        final NextKSketch nk2 = new NextKSketch(cso, topRow2, maxSize);
        final NextKList leftK2 = nk2.getNextKList(leftTable);
        IndexComparator leftComp2 = cso.getComparator(leftK2.table);
        for (int i = 0; i < (leftK2.table.getNumOfRows() - 1); i++)
            assertTrue(leftComp2.compare(i, i + 1) <= 0);
        //System.out.println(leftK2.toLongString(maxSize));
        final Table rightTable = getRepIntTable(rightSize, numCols);
        final NextKList rightK = nk.getNextKList(rightTable);
        IndexComparator rightComp = cso.getComparator(rightK.table);
        for (int i = 0; i < (rightK.table.getNumOfRows() - 1); i++)
            assertTrue(rightComp.compare(i, i + 1) <= 0);
        //System.out.println(rightK.toLongString(maxSize));
        final NextKList tK = nk.add(leftK, rightK);
        IndexComparator tComp = cso.getComparator(tK.table);
        for (int i = 0; i < (tK.table.getNumOfRows() - 1); i++)
            assertTrue(tComp.compare(i, i + 1) <= 0);
        //System.out.println(tK.toLongString(maxSize));
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final int maxSize = 50;
        final int leftSize = 1000;
        final Table leftTable = getRepIntTable(leftSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        //System.out.println(leftTable.toLongString(50));
        RecordOrder cso = new RecordOrder();
        final NextKSketch nk= new NextKSketch(cso, topRow, maxSize);
        final NextKList leftK = nk.getNextKList(leftTable);
        assertEquals(leftK.table.getNumOfRows(), 0);
    }

    @Test
    public void testTopK3() {
        //printTime("start");
        final int numCols = 3;
        final int maxSize = 50;
        final int bigSize = 100000;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(bigTable, 1000);
        //System.out.printf("Top Row %s. %n", topRow.toString());
        //printTime("created");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(bigTable.getSchema().getDescription(colName), true));
        }
        List<SmallTable> tabList = SplitTable(bigTable, 10000);
        //printTime("split");
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        //printTime("Parallel");
        NextKList nk = all.blockingSketch(new NextKSketch(cso, topRow, maxSize));
        IndexComparator mComp = cso.getComparator(nk.table);
        for (int i = 0; i < (nk.table.getNumOfRows() - 1); i++)
            assertTrue(mComp.compare(i, i + 1) <= 0);
        //System.out.println(nk.toLongString(maxSize));
    }

}

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

public class TopKSketchTest {
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
        final TopKSketch tkSketch = new TopKSketch(cso, maxSize);
        final NextKList leftK = tkSketch.getKList(leftTable);
        IndexComparator leftComp = cso.getComparator(leftK.table);
        for (int i = 0; i < (leftK.table.getNumOfRows() - 1); i++)
            assertTrue(leftComp.compare(i, i + 1) <= 0);
        //System.out.println(leftK.toLongString(maxSize));
        final Table rightTable = getRepIntTable(rightSize, numCols);
        final NextKList rightK = tkSketch.getKList(rightTable);
        IndexComparator rightComp = cso.getComparator(rightK.table);
        for (int i = 0; i < (rightK.table.getNumOfRows() - 1); i++)
            assertTrue(rightComp.compare(i, i + 1) <= 0);
        //System.out.println(rightK.toLongString(maxSize));
        final NextKList tK = tkSketch.add(leftK, rightK);
        IndexComparator tComp = cso.getComparator(tK.table);
        for (int i = 0; i < (tK.table.getNumOfRows() - 1); i++)
            assertTrue(tComp.compare(i, i + 1) <= 0);
        //System.out.println(tK.toLongString(maxSize));
    }

    @Test
    public void testTopK2() {
        //printTime("start");
        final int numCols = 3;
        final int maxSize = 50;
        final int bigSize = 100000;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
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
        NextKList tk = all.blockingSketch(new TopKSketch(cso, maxSize));
        IndexComparator mComp = cso.getComparator(tk.table);
        for (int i = 0; i < (tk.table.getNumOfRows() - 1); i++)
            assertTrue(mComp.compare(i, i + 1) <= 0);
        //System.out.println(tk.toLongString(maxSize));
    }
}


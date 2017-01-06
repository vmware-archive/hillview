package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.SparseMembership;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IndexComparator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.getIntTable;

public class QuantileSketchTest {

    private List<Table> SplitTable(Table bigTable, int fragmentSize) {
        int tableSize = bigTable.members.getSize();
        int numTables = tableSize/fragmentSize + 1;
        List<Table> tableList = new ArrayList<Table>(numTables);
        int start = 0;
        while (start < tableSize){
            int thisFragSize = Math.min(fragmentSize, tableSize - start);
            IMembershipSet members = new SparseMembership(start, thisFragSize);
            tableList.add(bigTable.compress(members));
            start += fragmentSize;
        }
        return tableList;
    }


    @Test
    public void testQuantile() {
        final int numCols = 2;
        final int leftSize = 100;
        final Table leftTable = getIntTable(leftSize, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : leftTable.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(leftTable.schema.getDescription(colName), true));
        }

        final int rightSize = 200;
        final Table rightTable = getIntTable(rightSize, numCols);
        final int resolution = 100;
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final QuantileList leftQ = qSketch.getQuantile(leftTable);
        final IndexComparator leftComp = cso.getComparator(leftQ.quantile);
        //System.out.println(leftQ);
        for (int i = 0; i < leftQ.getQuantileSize() - 1; i++)
            assertTrue(leftComp.compare(i, i + 1) <= 0);
        final QuantileList rightQ = qSketch.getQuantile(rightTable);
        //System.out.println(rightQ);
        final IndexComparator rightComp = cso.getComparator(rightQ.quantile);
        for (int i = 0; i < rightQ.getQuantileSize() - 1; i++)
            assertTrue(rightComp.compare(i, i + 1) <= 0);
        final QuantileList mergedQ = qSketch.add(leftQ, rightQ);
        IndexComparator mComp = cso.getComparator(mergedQ.quantile);
        for (int i = 0; i < mergedQ.getQuantileSize() - 1; i++)
            assertTrue(mComp.compare(i, i + 1) <= 0);
        int newSize = 20;
        final QuantileList approxQ = mergedQ.compressApprox(newSize);
        IndexComparator approxComp = cso.getComparator(approxQ.quantile);
        for (int i = 0; i < approxQ.getQuantileSize() - 1; i++)
            assertTrue(approxComp.compare(i, i + 1) <= 0);
        final QuantileList exactQ = mergedQ.compressExact(newSize);
        IndexComparator exactComp = cso.getComparator(exactQ.quantile);
        for (int i = 0; i < exactQ.getQuantileSize() - 1; i++)
            assertTrue(exactComp.compare(i, i + 1) <= 0);
    }

    private long last = 0;

    private void printTime(String when) {
        long now = System.currentTimeMillis();
        if (last > 0)
            System.out.println(when + " " + (now - last));
        last = now;
    }

    @Test
    public void testQuantile1() {
        printTime("start");
        final int numCols = 3;
        final int resolution = 49;
        final int bigSize = 10000000;
        final Table bigTable = getIntTable(bigSize, numCols);
        printTime("created");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(bigTable.schema.getDescription(colName), true));
        }
        List<Table> tabList = this.SplitTable(bigTable, 1000000);
        printTime("split");
        // Create a big parallel data set containing all table fragments
        ArrayList<IDataSet<Table>> a = new ArrayList<IDataSet<Table>>();
        for (Table t : tabList) {
            LocalDataSet<Table> ds = new LocalDataSet<Table>(t);
            a.add(ds);
        }
        ParallelDataSet<Table> all = new ParallelDataSet<Table>(a);
        printTime("Parallel");
        QuantileList ql = all.blockingSketch(new QuantileSketch(cso, resolution)).
                compressExact(resolution);
        printTime("Quantile");
        /*
        IRowOrder order = new ArrayRowOrder(cso.getSortedRowOrder(bigTable));
        printTime("sort");
        Table sortTable = bigTable.compress(order);
        //System.out.println(sortTable.toLongString(50));
        //System.out.println(ql.quantile.toLongString(50));
        printTime("Compress");
        int j =0, lastMatch = 0;
        for (int i =0; i < ql.getQuantileSize(); i++) {
            boolean match = false;
            while(match == false) {
                match = true;
                for (String colName : ql.getSchema().getColumnNames()) {
                    if (ql.getColumn(colName).getInt(i) !=
                            sortTable.getColumn(colName).getInt(j))
                        match = false;
                }
                if (match == true) {
                    System.out.printf("Rank %f, target %f %n",
                            ((j+1)/ ((double) bigSize +1)), (i + 1)/ (double) (resolution + 1));
                    lastMatch = j;
                }
                j++;
                if(j >= bigSize) {
                    System.out.printf("Error! No match for %s %n", new RowSnapshot(ql.quantile, i).toString());
                    //System.out.println(sortTable.toLongString(lastMatch,50));
                    j = 0;
                    break;
                }
            }
        }
        printTime("done");
        */
    }

    /*
    @Test
    public void testQuantileSample() {
        printTime("start");
        final int numCols = 3;
        final int resolution = 99;
        final int bigSize = 1000;
        final Table bigTable = getIntTable(bigSize, numCols);
        printTime("created");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(bigTable.schema.getDescription(colName), true));
        }
        QuantileList ql = new QuantileSketch(cso, resolution).getQuantile(bigTable).compressExact(resolution);
        printTime("Quantile");
        IRowOrder order = new ArrayRowOrder(cso.getSortedRowOrder(bigTable));
        printTime("sort");
        Table sortTable = bigTable.compress(order);
        printTime("compressed");
        int j =0;
        for (int i =0; i < resolution; i++) {
            boolean match = false;
            while(match == false) {
                match = true;
                for (String colName : ql.getSchema().getColumnNames()) {
                    if (ql.getColumn(colName).getObject(i) != sortTable.getColumn(colName).getObject(j))
                        match = false;
                }
                if (match == true) {
                //    System.out.printf("%d has rank: %f, %n", i + 1, (j * 100.0) / bigSize);
                }
                j++;
                if(j >= bigSize) {
                    System.out.printf("Error! No match for %n", i + 1);
                    j = 0;
                    break;
                }
            }
        }
        printTime("done");
    }
    */
}
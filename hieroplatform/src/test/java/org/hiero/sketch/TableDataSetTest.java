package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IndexComparator;
import org.junit.Test;

import java.util.ArrayList;

import static junit.framework.TestCase.assertTrue;

public class TableDataSetTest {
    @Test
    public void localDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final Table randTable = TableTest.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable.schema.getDescription(colName), true));
        }
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final LocalDataSet<Table> ld = new LocalDataSet<Table>(randTable);
        final QuantileList ql = ld.blockingSketch(qSketch);
        IndexComparator comp = cso.getComparator(ql.quantile);
        for (int i = 0; i < ql.getQuantileSize() - 1; i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
        //System.out.println(ql);
    }

    @Test
    public void parallelDataSetTest() {
        final int numCols = 3;
        final int size = 1000, resolution = 20;
        final Table randTable1 = TableTest.getIntTable(size, numCols);
        final Table randTable2 = TableTest.getIntTable(size, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : randTable1.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(randTable1.schema.getDescription(colName), true));
        }

        final LocalDataSet<Table> ld1 = new LocalDataSet<Table>(randTable1);
        final LocalDataSet<Table> ld2 = new LocalDataSet<Table>(randTable2);
        final ArrayList<IDataSet<Table>> elems = new ArrayList<IDataSet<Table>>(2);
        elems.add(ld1);
        elems.add(ld2);
        final ParallelDataSet<Table> par = new ParallelDataSet<Table>(elems);
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final QuantileList r = par.blockingSketch(qSketch);
        IndexComparator comp = cso.getComparator(r.quantile);
        for (int i = 0; i < r.getQuantileSize() - 1; i++)
            assertTrue(comp.compare(i, i + 1) <= 0);
        //System.out.println(r);
    }
}
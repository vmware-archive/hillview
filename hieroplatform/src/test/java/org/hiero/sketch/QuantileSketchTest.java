package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.ColumnOrientation;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.ColumnSortOrder;
import org.hiero.sketch.table.ListComparator;
import org.hiero.sketch.table.Table;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.getIntTable;

public class QuantileSketchTest {

    @Test
    public void testQuantile() {
        final int numCols = 2;
        final List<ColumnOrientation> sortOrder = new ArrayList<ColumnOrientation>(numCols);
        for (int i = 0; i < numCols; i++) {
            sortOrder.add(i, new ColumnOrientation("Column" + String.valueOf(i), ((i % 2) == 0)));
        }
        final int leftSize = 1000;
        final Table leftTable = getIntTable(leftSize, numCols);
        final int rightSize = 2000;
        final Table rightTable = getIntTable(rightSize, numCols);
        final int resolution = 10;
        final QuantileSketch qSketch = new QuantileSketch(sortOrder, resolution);
        final QuantileList leftQ = qSketch.getQuantile(leftTable);
        final ListComparator leftComp = new ColumnSortOrder(sortOrder).getComparator(leftQ.quantile);
        for(int i =0; i < leftQ.getQuantileSize() -1; i++ )
            assertTrue(leftComp.compare(i,i+1) <= 0);
        final QuantileList rightQ = qSketch.getQuantile(rightTable);
        final ListComparator rightComp = new ColumnSortOrder(sortOrder).getComparator(rightQ.quantile);
        for(int i =0; i < rightQ.getQuantileSize() -1; i++ )
            assertTrue(rightComp.compare(i,i+1) <= 0);
        final QuantileList mergedQ = qSketch.add(leftQ, rightQ);
        final ListComparator mComp = new ColumnSortOrder(sortOrder).getComparator(mergedQ.quantile);
        for(int i =0; i < mergedQ.getQuantileSize() -1; i++ )
            assertTrue(mComp.compare(i,i+1) <= 0);
    }
}
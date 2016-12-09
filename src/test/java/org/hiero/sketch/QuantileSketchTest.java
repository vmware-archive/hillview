package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.OrderedColumn;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.Table;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hiero.sketch.TableTest.getIntTable;

public class QuantileSketchTest {

    @Test
    public void testQuantile() {
        final int numCols = 2;
        final List<OrderedColumn> sortOrder = new ArrayList<OrderedColumn>(numCols);
        for (int i = 0; i < numCols; i++) {
            sortOrder.add(i, new OrderedColumn("Column" + String.valueOf(i), ((i % 2) == 0)));
        }
        final int leftSize = 1000;
        final Table leftTable = getIntTable(leftSize, numCols);
        final int rightSize = 1000;
        final Table rightTable = getIntTable(rightSize, numCols);
        final QuantileSketch qSketch = new QuantileSketch(sortOrder);
        final int resolution = 10;
        final QuantileList leftQ = qSketch.getQuantile(leftTable, resolution);
        //System.out.println(leftQ.toString());
        final QuantileList rightQ = qSketch.getQuantile(rightTable, resolution);
        //System.out.println(rightQ.toString());
        final QuantileList mergedQ = qSketch.mergeQuantiles(leftQ, rightQ);
        //System.out.println(mergedQ.toString());
    }
}
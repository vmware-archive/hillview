package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IndexComparator;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.getIntTable;

public class QuantileSketchTest {

    @Test
    public void testQuantile() {
        final int numCols = 2;
        final int leftSize = 1000;
        final Table leftTable = getIntTable(leftSize, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName: leftTable.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(leftTable.schema.getDescription(colName), true));
        }

        final int rightSize = 2000;
        final Table rightTable = getIntTable(rightSize, numCols);
        final int resolution = 100;
        final QuantileSketch qSketch = new QuantileSketch(cso, resolution);
        final QuantileList leftQ = qSketch.getQuantile(leftTable);
        final IndexComparator leftComp = cso.getComparator(leftQ.quantile);
        //System.out.println(leftQ);
        for(int i = 0; i < leftQ.getQuantileSize() -1; i++ )
            assertTrue(leftComp.compare(i,i+1) <= 0);
        final QuantileList rightQ = qSketch.getQuantile(rightTable);
        //System.out.println(rightQ);
        final IndexComparator rightComp = cso.getComparator(rightQ.quantile);
        for(int i = 0; i < rightQ.getQuantileSize() -1; i++ )
            assertTrue(rightComp.compare(i,i+1) <= 0);
        final QuantileList mergedQ = qSketch.add(leftQ, rightQ);
        IndexComparator mComp = cso.getComparator(mergedQ.quantile);
        for(int i = 0; i < mergedQ.getQuantileSize() -1; i++ )
            assertTrue(mComp.compare(i,i+1) <= 0);
        int newSize = 20;
        final QuantileList approxQ = mergedQ.compressApprox(newSize);
        IndexComparator approxComp = cso.getComparator(approxQ.quantile);
        for(int i = 0; i < approxQ.getQuantileSize() -1; i++ )
            assertTrue(approxComp.compare(i,i+1) <= 0);
        final QuantileList exactQ = mergedQ.compressExact(newSize);
        IndexComparator exactComp = cso.getComparator(exactQ.quantile);
        for(int i = 0; i < exactQ.getQuantileSize() -1; i++ )
            assertTrue(exactComp.compare(i,i+1) <= 0);
    }
}
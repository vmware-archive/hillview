package org.hiero.sketch;

import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.NextKList;
import org.hiero.sketch.spreadsheet.TopKSketch;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.Table;
import org.junit.Test;

import static org.hiero.sketch.TableTest.getRepIntTable;

public class TopKSketchTest {


    @Test
    public void testTopK() {
        final int numCols = 2;
        final int maxSize = 50;
        final int rightSize = 1000;
        final int leftSize = 1000;
        final Table leftTable = getRepIntTable(leftSize, numCols);
        //System.out.println(leftTable.toLongString(50));
        RecordOrder cso = new RecordOrder();
        for (String colName : leftTable.schema.getColumnNames()) {
            cso.append(new ColumnSortOrientation(leftTable.schema.getDescription(colName), true));
        }
        final TopKSketch tkSketch = new TopKSketch(cso, maxSize);
        final NextKList leftK = tkSketch.getKList(leftTable);
        System.out.println(leftK.toLongString(maxSize));
        final Table rightTable = getRepIntTable(rightSize, numCols);
        final NextKList rightK = tkSketch.getKList(rightTable);
        System.out.println(rightK.toLongString(maxSize));
        final NextKList tK = tkSketch.add(leftK, rightK);
        System.out.println(tK.toLongString(maxSize));
    }
}


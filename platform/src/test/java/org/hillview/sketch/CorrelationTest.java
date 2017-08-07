package org.hillview.sketch;

import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.BasicColStats;
import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.junit.Test;

import java.util.*;

public class CorrelationTest {
    @Test
    public void testCorrelation() {
        ITable table = TestTables.getCorrelatedCols(300, 30, 10);
        List<String> colNames = new ArrayList<String>(table.getSchema().getColumnNames());
        Map<String, BasicColStats> bcsMap = new HashMap<String, BasicColStats>(table.getSchema().getColumnCount());
        for (IColumn col : table.getColumns()) {
            BasicColStatSketch statSketch = new BasicColStatSketch(col.getName(), null);
            BasicColStats bcs = statSketch.create(table);
            bcsMap.put(col.getName(), bcs);
        }

        FullCorrelationSketch fcs = new FullCorrelationSketch(colNames, bcsMap);
        CorrMatrix corrMatrix = fcs.create(table);
        System.out.println(Arrays.deepToString(corrMatrix.getCorrelationMatrix()));
    }
}

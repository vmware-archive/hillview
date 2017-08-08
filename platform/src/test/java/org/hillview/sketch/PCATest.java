package org.hillview.sketch;

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.BasicColStats;
import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.LinAlg;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PCATest {
    @Test
    public void testLinearDataset() {
        ITable table = TestTables.getLinearTable(10000, 30);
        List<String> colNames = new ArrayList<String>(table.getSchema().getColumnNames());

        IDataSet<ITable> dataset = TestTables.makeParallel(table, 1000);

        Map<String, BasicColStats> bcsMap = new HashMap<String, BasicColStats>(table.getSchema().getColumnCount());
        for (IColumn col : table.getColumns()) {
            BasicColStatSketch statSketch = new BasicColStatSketch(col.getName(), null);
            BasicColStats bcs = dataset.blockingSketch(statSketch);
            bcsMap.put(col.getName(), bcs);
        }

        FullCorrelationSketch fcs = new FullCorrelationSketch(colNames, bcsMap);
        CorrMatrix cm = dataset.blockingSketch(fcs);

        DoubleMatrix corrMatrix = new DoubleMatrix(cm.getCorrelationMatrix());
        // Get just the eigenvector corresponding to the largest eigenvalue (because we know the data is approximately
        // linear).
        DoubleMatrix eigenVectors = LinAlg.eigenVectors(corrMatrix, 1);
        eigenVectors.print();

        for (int i = 2; i < eigenVectors.columns; i++) {
            // The eigenvector should have reasonably large components in the first two columns, compared to the
            // other components in the eigenvector.
            Assert.assertTrue(
                    "First component of eigenvector not large enough.",
                    Math.abs(eigenVectors.get(0, 0)) > 3 * Math.abs(eigenVectors.get(0, i))
            );
            Assert.assertTrue(
                    "Second component of eigenvector not large enough.",
                    Math.abs(eigenVectors.get(0, 1)) > 3 * Math.abs(eigenVectors.get(0, i))
            );
        }
    }
}

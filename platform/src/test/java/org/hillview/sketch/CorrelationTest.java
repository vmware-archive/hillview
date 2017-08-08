package org.hillview.sketch;

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.BasicColStats;
import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.LinAlg;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class CorrelationTest {
    @Test
    public void testCorrelation() {
        DoubleMatrix mat = new DoubleMatrix(new double[][]{{9, 8, 4, 1, 6}, {5, 8, 2, 10, 1}, {6, 4, 1, 6, 5}});
        ITable table = BlasConversions.toTable(mat);
        List<String> colNames = new ArrayList<String>(table.getSchema().getColumnNames());
        Map<String, BasicColStats> bcsMap = new HashMap<String, BasicColStats>(table.getSchema().getColumnCount());
        for (IColumn col : table.getColumns()) {
            BasicColStatSketch statSketch = new BasicColStatSketch(col.getName(), null);
            BasicColStats bcs = statSketch.create(table);
            bcsMap.put(col.getName(), bcs);
        }

        FullCorrelationSketch fcs = new FullCorrelationSketch(colNames, bcsMap);
        CorrMatrix cm = fcs.create(table);

        DoubleMatrix corrMatrix = new DoubleMatrix(cm.getCorrelationMatrix());
        DoubleMatrix eigenVectors = LinAlg.eigenVectors(corrMatrix, 2);
    }

    @Test
    public void testBigCorrelation() {
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

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
import org.jblas.util.Random;
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
        Random.seed(43);
        DoubleMatrix mat = DoubleMatrix.rand(10000, 30);
        ITable bigTable = BlasConversions.toTable(mat);
        List<String> colNames = new ArrayList<String>(bigTable.getSchema().getColumnNames());
        IDataSet<ITable> dataset = TestTables.makeParallel(bigTable, 100);


        Map<String, BasicColStats> bcsMap = new HashMap<String, BasicColStats>(bigTable.getSchema().getColumnCount());
        for (IColumn col : bigTable.getColumns()) {
            BasicColStatSketch statSketch = new BasicColStatSketch(col.getName(), null);
            BasicColStats bcs = dataset.blockingSketch(statSketch);
            bcsMap.put(col.getName(), bcs);
        }

        FullCorrelationSketch fcs = new FullCorrelationSketch(colNames, bcsMap);
        CorrMatrix cm = dataset.blockingSketch(fcs);
        CorrMatrix cmCheck = fcs.create(bigTable);
        for (int i = 0; i < cm.getCorrelationMatrix().length; i++) {
            double[] row = cm.getCorrelationMatrix()[i];
            for (int j = 0; j < row.length; j++) {
                double actual = cm.getCorrelationMatrix()[i][j];
                double expected = cmCheck.getCorrelationMatrix()[i][j];
                Assert.assertTrue(
                        "Correlation differs too much from check.",
                        Math.abs(actual - expected) / Math.abs(expected) < 1e-10
                );
            }
        }
    }
}

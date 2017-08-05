package org.hillview.sketch;

import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.LinearProjectionMap;
import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.BasicColStats;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class LinearProjectionTest {

    @Test
    public void testConversions() {
        int cols = 20;
        int rows = 2000;
        Random.seed(42);
        DoubleMatrix matrix = DoubleMatrix.rand(rows, cols);
        ITable table = TestTables.fromDoubleMatrix(matrix);
        DoubleMatrix matrix2 = TestTables.toDoubleMatrix(table);
        Assert.assertEquals(rows * cols, matrix.eq(matrix2).sum(), Math.ulp(rows * rows));
    }

    @Test
    public void testProjection() {
        int cols = 20;
        int rows = 2000;
        int numProjections = 8;
        Random.seed(42);
        DoubleMatrix matrix = DoubleMatrix.rand(rows, cols);
        DoubleMatrix projectionMatrix = DoubleMatrix.rand(numProjections, cols);
        ITable table = TestTables.fromDoubleMatrix(matrix);
        LinearProjectionMap lpm = new LinearProjectionMap(table.getSchema().getColumnNames().toArray(new String[]{}), projectionMatrix);
        ITable result = lpm.apply(table);

        String[] newColNames = new String[numProjections];
        for (int i = 0; i < numProjections; i++) {
            newColNames[i] = String.format("LinearProjection%d", i);
        }

        DoubleMatrix projectedData = result.getNumericMatrix(newColNames, null);
        DoubleMatrix projectedDataCheck = matrix.mmul(projectionMatrix.transpose());
        Assert.assertEquals(rows * numProjections, projectedData.eq(projectedDataCheck).sum(), Math.ulp(rows * cols));
    }

    @Test
    public void testDatasetProjection() {
        int rows = 10000;
        int cols = 20;
        int numProjections = 2;
        DoubleMatrix dataMatrix = DoubleMatrix.rand(rows, cols);
        DoubleMatrix projectionMatrix = DoubleMatrix.rand(numProjections, cols);
        DoubleMatrix projectionCheck = dataMatrix.mmul(projectionMatrix.transpose());

        ITable bigTable = TestTables.fromDoubleMatrix(dataMatrix);
        String[] colNames = bigTable.getSchema().getColumnNames().toArray(new String[]{});
        // Convert it to an IDataset
        IDataSet<ITable> all = TestTables.makeParallel(bigTable, rows / 10);

        LinearProjectionMap lpm = new LinearProjectionMap(colNames, projectionMatrix);
        IDataSet<ITable> result = all.blockingMap(lpm);

        for (int i = 0; i < numProjections; i++) {
            BasicColStatSketch bcss = new BasicColStatSketch(String.format("LinearProjection%d", i), null);
            BasicColStats bcs = result.blockingSketch(bcss);
            double expectedMean = projectionCheck.get(new AllRange(), i).mean();
            double actualMean = bcs.getMoment(1);
            double eps = actualMean * 1e-6;
            Assert.assertTrue("Mean is too far from actual mean", Math.abs(actualMean - expectedMean) < eps);

            double expectedMin = projectionCheck.get(new AllRange(), i).min();
            double actualMin = bcs.getMin();
            eps = actualMin * 1e-6;
            Assert.assertTrue("Min is too far from actual min", Math.abs(actualMin - expectedMin) < eps);

            double expectedMax = projectionCheck.get(new AllRange(), i).max();
            double actualMax = bcs.getMax();
            eps = actualMax* 1e-6;
            Assert.assertTrue("Max is too far from actual min", Math.abs(actualMax - expectedMax) < eps);
        }
    }
}

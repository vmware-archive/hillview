package org.hillview.sketch;

import org.hillview.maps.LinearProjectionMap;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
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
}

package org.hillview.sketch;

import org.hillview.table.ExplicitStringConverter;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

public class MatrixTest {
    @Test
    public void testSimpleMatrix() {
        // Setting elements
        DoubleMatrix mat = new DoubleMatrix(2, 2);
        mat.put(0, 0, 2.3);
        mat.put(1, 0, 42.42);
        mat.put(0, 1, 3.14);
        mat.put(1, 1, 3.91992);
        Assert.assertEquals(mat.get(0, 0), 2.3, Math.ulp(2.3));
        Assert.assertEquals(mat.get(1, 0), 42.42, Math.ulp(42.42));
        Assert.assertEquals(mat.get(0, 1), 3.14, Math.ulp(3.14));
        Assert.assertEquals(mat.get(1, 1), 3.91992, Math.ulp(3.91992));

        // Matrix multiplied with its transpose should be symmetric.
        DoubleMatrix symMat = mat.mmul(mat.transpose());
        for (int i = 0; i < symMat.rows; i++) {
            for (int j = 0; j < symMat.columns; j++) {
                Assert.assertEquals(symMat.get(i, j), symMat.get(j, i), Math.ulp(symMat.get(i, j)));
            }
        }
    }

    @Test
    public void testColumnFetch() {
        ITable table = TestTables.testRepTable();
        DoubleMatrix mat = table.getNumericColumn("Age", new ExplicitStringConverter());
        IRowIterator it = table.getRowIterator();
        int row = it.getNextRow();
        int i = 0;
        while (row >= 0) {
            Assert.assertEquals(
                    mat.get(i),
                    table.getColumn("Age").asDouble(row, new ExplicitStringConverter()),
                    Math.ulp(mat.get(i))
            );
            row = it.getNextRow();
            i++;
        }
        Assert.assertEquals(i, mat.rows);
    }

    @Test
    public void testMatrixFetch() {
        ITable table = TestTables.getIntTable(100, 3);
        String[] colNames = {"Column0", "Column1"};

        DoubleMatrix mat = table.getNumericMatrix(colNames, new ExplicitStringConverter());
        IRowIterator it = table.getRowIterator();
        int row = it.getNextRow();
        int i = 0;
        while (row >= 0) {
            for (int j = 0; j < colNames.length; j++) {
                Assert.assertEquals(
                        mat.get(i, j),
                        table.getColumn(colNames[j]).asDouble(row, new ExplicitStringConverter()),
                        Math.ulp(mat.get(i, j))
                );
            }
            row = it.getNextRow();
            i++;
        }
        Assert.assertEquals(i, mat.rows);
    }
}

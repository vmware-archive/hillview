package org.hillview.sketch;

import org.hillview.table.ColumnDescription;
import org.hillview.table.DoubleArrayColumn;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.BlasConversions;
import org.hillview.utils.TestTables;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

@SuppressWarnings("ConstantConditions")
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
    public void testMatrixFetch() {
        ITable table = TestTables.getIntTable(100, 3);
        List<String> colNames = new ArrayList<String>(Arrays.asList("Column0", "Column1"));

        DoubleMatrix mat = BlasConversions.toDoubleMatrix(table, colNames, null);
        IRowIterator it = table.getRowIterator();
        int row = it.getNextRow();
        int i = 0;
        while (row >= 0) {
            for (int j = 0; j < colNames.size(); j++) {
                Assert.assertEquals(
                        mat.get(i, j),
                        table.getColumn(colNames.get(j)).asDouble(row, null),
                        Math.ulp(mat.get(i, j))
                );
            }
            row = it.getNextRow();
            i++;
        }
        Assert.assertEquals(i, mat.rows);
    }

    @Test
    public void testMissingConversion() {
        int numCols = 5;
        int[] missing = {3, 5, 12, 20, 0};
        int numRows = 30;

        List<IColumn> columns = new ArrayList<IColumn>();
        for (int i = 0; i < numCols; i++) {
            ColumnDescription description = new ColumnDescription("Column" + i, ContentsKind.Double, true);
            DoubleArrayColumn column = new DoubleArrayColumn(description, numRows);
            for (int j = 0; j < missing[i]; j++) {
                column.setMissing(j);
            }
            columns.add(column);
        }

        ITable table = new Table(columns);
        List<String> colNames = new ArrayList<String>(table.getSchema().getColumnNames());
        DoubleMatrix mat = BlasConversions.toDoubleMatrixMissing(table, colNames, null, Double.NaN);
        DoubleMatrix missingCount = mat.isNaN().columnSums();
        for (int i = 0; i < numCols; i++) {
            Assert.assertEquals(missing[i], Math.round(missingCount.get(i)));
        }
    }
}

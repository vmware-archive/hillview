package org.hillview.utils;

import org.hillview.table.ColumnDescription;
import org.hillview.table.DoubleArrayColumn;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.PointRange;

import javax.annotation.Nullable;
import java.util.Arrays;

/**
 * This class contains methods that convert from/to Tables to/from DoubleMatrices. These methods copy all data, so they
 * should only be used when it is known that the Table is reasonably small.
 */
public class BlasConversions {
    /**
     * Convert from an ITable to a DoubleMatrix. This copies all data from the table.
     * @param table Table that is to be converted.
     * @param colNames Names of columns in the table that have to be converted.
     * @param converter String converter for converting string data to numeric data.
     * @return DoubleMatrix with the table's columns interpreted as doubles.
     */
    public static DoubleMatrix toDoubleMatrix(ITable table, String[] colNames, @Nullable IStringConverter converter) {
        DoubleMatrix mat = new DoubleMatrix(table.getNumOfRows(), colNames.length);
        IRowIterator iter = table.getRowIterator();
        int row = iter.getNextRow();
        int i = 0;
        while (row >= 0) {
            for (int j = 0; j < colNames.length; j++) {
                mat.put(i, j, table.getColumn(colNames[j]).asDouble(row, converter));
            }
            row = iter.getNextRow();
            i++;
        }
        return mat;
    }

    /**
     * Convert from a DoubleMatrix to a Table. This copies all the data.
     * @param mat Matrix with numeric data that has to be in the table.
     * @return Table with the numeric data from mat. Column names are set to 'Column{i}'.
     */
    public static Table toTable(DoubleMatrix mat) {
        IColumn[] columns = new IColumn[mat.columns];
        for (int i = 0; i < mat.columns; i++) {
            ColumnDescription cd = new ColumnDescription(String.format("Column%d", i), ContentsKind.Double, false);
            DoubleMatrix vector = mat.get(new AllRange(), new PointRange(i));
            IColumn column = new DoubleArrayColumn(cd, vector.data);
            columns[i] = column;
        }
        return new Table(Arrays.asList(columns));
    }

}

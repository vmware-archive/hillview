package org.hillview.utils;

import org.hillview.table.ColumnDescription;
import org.hillview.table.DoubleArrayColumn;
import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.PointRange;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public static DoubleMatrix toDoubleMatrix(ITable table, List<String> colNames, @Nullable IStringConverter
            converter) {
        DoubleMatrix mat = new DoubleMatrix(table.getNumOfRows(), colNames.size());
        for (int j = 0; j < colNames.size(); j++) {
            IColumn col = table.getColumn(colNames.get(j));
            IRowIterator iter = table.getRowIterator();
            int row = iter.getNextRow();
            int i = 0;
            while (row >= 0) {
                mat.put(i, j, col.asDouble(row, converter));
                row = iter.getNextRow();
                i++;
            }
        }
        return mat;
    }

    /**
     * Convert from an ITable to a DoubleMatrix. This copies all data from the table.
     * @param table Table that is to be converted.
     * @param colNames Names of columns in the table that have to be converted.
     * @param converter String converter for converting string data to numeric data.
     * @param missingValue Value to put in the matrix where missing values occur.
     * @return Array of DoubleMatrices. The first one is
     */
    public static DoubleMatrix toDoubleMatrixMissing(ITable table, List<String> colNames, @Nullable IStringConverter
            converter, double missingValue) {
        DoubleMatrix mat = new DoubleMatrix(table.getNumOfRows(), colNames.size());
        DoubleMatrix missing = DoubleMatrix.zeros(colNames.size());
        for (int j = 0; j < colNames.size(); j++) {
            IColumn col = table.getColumn(colNames.get(j));
            IRowIterator iter = table.getRowIterator();
            int row = iter.getNextRow();
            int i = 0;
            while (row >= 0) {
                try {
                    mat.put(i, j, col.asDouble(row, converter));
                } catch (MissingException e) {
                    missing.put(j, missing.get(j) + 1);
                    mat.put(i, j, missingValue);
                }
                row = iter.getNextRow();
                i++;
            }
        }
        return mat;
    }

    /**
     * Convert from a DoubleMatrix to a Table. This copies all the data.
     * @param mat Matrix with numeric data that has to be in the table.
     * @param colNames A list with the column names of the newly created table.
     * @return Table with the numeric data from mat. Column names are set to '{columnNames[i]}'.
     */
    public static Table toTable(DoubleMatrix mat, List<String> colNames) {
        IColumn[] columns = new IColumn[mat.columns];
        for (int i = 0; i < mat.columns; i++) {
            ColumnDescription cd = new ColumnDescription(colNames.get(i), ContentsKind.Double, false);
            DoubleMatrix vector = mat.get(new AllRange(), new PointRange(i));
            IColumn column = new DoubleArrayColumn(cd, vector.data);
            columns[i] = column;
        }
        return new Table(Arrays.asList(columns));
    }

    /**
     * Calls the above toTable method with column names 'Column{i}'.
     * @param mat Matrix that has to be converted to a table.
     * @return Table with the numeric data from mat. Column names are set to 'Column{i}'.
     */
    public static Table toTable(DoubleMatrix mat) {
        List<String> colNames = new ArrayList<String>();
        for (int i = 0; i < mat.columns; i++) {
            colNames.add("Column" + i);
        }
        return toTable(mat, colNames);
    }

}

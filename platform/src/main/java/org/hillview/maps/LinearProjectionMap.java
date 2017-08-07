package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.jblas.DoubleMatrix;

import java.util.ArrayList;
import java.util.List;

/**
 * This map takes a list of column names and a projection matrix and applies the projection matrix to the matrix that is
 * constructed from the table by horizontally stacking the specified columns. The resulting table is a copy of the old
 * table, with the additional projected columns added to it. The new columns are named 'LinearProjection{i}'.
 */
public class LinearProjectionMap implements IMap<ITable, ITable> {
    /**
     * The projection matrix is structured as follows: Every row is a vector
     * that is projected on. The ordering of the columns is the same as the order
     * of the column names in colNames.
     */
    private final DoubleMatrix projectionMatrix;
    private final String[] colNames;
    private final int numProjections;
    private final IStringConverter converter;

    public LinearProjectionMap(String[] colNames, DoubleMatrix projectionMatrix, IStringConverter converter) {
        if (colNames.length != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.numProjections = projectionMatrix.rows;
        this.converter = converter;
    }

    @Override
    public ITable apply(ITable table) {
        // Copy all existing columns to the column list for the new table.
        List<IColumn> columns = new ArrayList<IColumn>();
        Iterable<IColumn> inputColumns = table.getColumns();
        for (IColumn inputColumn : inputColumns) {
            columns.add(inputColumn);
        }

        // Compute and add all the projections to the columns.
        for (int j = 0; j < this.numProjections; j++) {
            String colName = String.format("LinearProjection%d", j);
            ColumnDescription colDesc = new ColumnDescription(colName, ContentsKind.Double, true);
            DoubleArrayColumn column = new DoubleArrayColumn(colDesc, table.getMembershipSet().getMax());
            IRowIterator it = table.getMembershipSet().getIterator();
            int row = it.getNextRow();
            while (row >= 0) {
                // Compute the dot product between the row from the table and the j'th projection vector.
                double x = 0.0;
                for (int k = 0; k < this.projectionMatrix.columns; k++) {
                    x += table.getColumn(this.colNames[k]).asDouble(row, this.converter) * this.projectionMatrix.get(j, k);
                }
                column.set(row, x);
                row = it.getNextRow();
            }
            columns.add(column);
        }

        return new Table(columns, table.getMembershipSet());
    }
}

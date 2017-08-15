package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.utils.BlasConversions;
import org.jblas.DoubleMatrix;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * This map takes a list of column names and a projection matrix and applies the projection matrix to the matrix that is
 * constructed from the table by horizontally stacking the specified columns. The resulting table is a copy of the old
 * table, with the additional projected columns added to it. The new columns are named '{newColName}{i}'.
 */
public class LinearProjectionMap implements IMap<ITable, ITable> {
    private static final Logger LOG = Logger.getLogger(LinearProjectionMap.class.getName());
    /**
     * The projection matrix is structured as follows: Every row is a vector
     * that is projected on. The ordering of the columns is the same as the order
     * of the column names in colNames.
     */
    private final DoubleMatrix projectionMatrix;
    private final List<String> colNames;
    private final String newColName;
    private final int numProjections;
    @Nullable
    private final IStringConverter converter;

    public LinearProjectionMap(List<String> colNames, DoubleMatrix projectionMatrix, String projectionName,
                               @Nullable IStringConverter converter) {
        if (colNames.size() != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.newColName = projectionName;
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

        // Compute the projection with BLAS
        DoubleMatrix mat = BlasConversions.toDoubleMatrix(table, this.colNames.toArray(new String[]{}), this.converter);
        DoubleMatrix resultMat = mat.mmul(this.projectionMatrix.transpose());

        // Copy the result to new columns with the same membershipset size. (Can't use BlasConversions here.)
        for (int j = 0; j < this.numProjections; j++) {
            ColumnDescription colDesc = new ColumnDescription(this.newColName + j, ContentsKind.Double, true);
            // TODO: create and use a SparseColumn
            DoubleArrayColumn column = new DoubleArrayColumn(colDesc, table.getMembershipSet().getMax());
            IRowIterator it = table.getMembershipSet().getIterator();
            int row = it.getNextRow();
            int i = 0;
            while (row >= 0) {
                column.set(row, resultMat.get(i, j));
                row = it.getNextRow();
                i++;
            }
            columns.add(column);
        }

        return new Table(columns, table.getMembershipSet());
    }
}

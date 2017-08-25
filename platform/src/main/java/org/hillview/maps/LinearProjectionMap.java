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
    private final List<String> newColNames;
    private final int numProjections;
    @Nullable
    private final IStringConverter converter;
    private static final double threshold = .3;
    // For columns sparser than this use sparse storage.

    public LinearProjectionMap(List<String> colNames, DoubleMatrix projectionMatrix, String projectionName,
                               @Nullable IStringConverter converter) {
        if (colNames.size() != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.newColNames = new ArrayList<String>();
        for (int i = 0; i < projectionMatrix.rows; i++) {
            newColNames.add(projectionName + i);
        }
        this.numProjections = projectionMatrix.rows;
        this.converter = converter;
    }

    public LinearProjectionMap(List<String> colNames, DoubleMatrix projectionMatrix, List<String> newColNames,
                               @Nullable IStringConverter converter) {
        if (colNames.size() != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.newColNames = newColNames;
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
        DoubleMatrix mat = BlasConversions.toDoubleMatrixMissing(table, this.colNames, this
                .converter, Double.NaN);
        DoubleMatrix resultMat = mat.mmul(this.projectionMatrix.transpose());

        // Copy the result to new columns with the same membershipSet size. (Can't use
        // BlasConversions here.)
        for (int j = 0; j < this.numProjections; j++) {
            ColumnDescription colDesc = new ColumnDescription(this.newColNames.get(j), ContentsKind.Double, true);
            int colSize = table.getMembershipSet().getMax();
            int colUse = table.getMembershipSet().getSize();
            IMutableColumn column;
            if (colUse * threshold < colSize)
                column = new SparseColumn(colDesc, colSize);
            else
                column = new DoubleArrayColumn(colDesc, colSize);
            IRowIterator it = table.getMembershipSet().getIterator();
            int row = it.getNextRow();
            int i = 0;
            while (row >= 0) {
                if (Double.isNaN(resultMat.get(i, j))) {
                    column.setMissing(row);
                } else {
                    column.set(row, resultMat.get(i, j));
                }
                row = it.getNextRow();
                i++;
            }
            columns.add(column);
        }

        return new Table(columns, table.getMembershipSet());
    }
}

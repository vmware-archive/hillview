package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.ranges.PointRange;

import java.util.ArrayList;
import java.util.List;

/**
 * This map takes a list of column names and a projection matrix,
 * and applies the projection matrix to the matrix that is
 * constructed from the table by horizontally stacking the
 * specified columns. The resulting table is a copy of the old
 * table, with the additional projected columns added to it.
 */
public class LinearProjectionMap implements IMap<ITable, ITable> {

    private final DoubleMatrix projectionMatrix;
    private final String[] colNames;
    private final int numLowDims;

    public LinearProjectionMap(String[] colNames, DoubleMatrix projectionMatrix) {
        if (colNames.length != projectionMatrix.columns)
            throw new RuntimeException("Number of columns in projectionMatrix should be eq. to number of names in colNames.");

        this.projectionMatrix = projectionMatrix;
        this.colNames = colNames;
        this.numLowDims = projectionMatrix.rows;
    }

    @Override
    public ITable apply(ITable data) {
        // The data matrix that has the observations as rows
        DoubleMatrix mat = data.getNumericMatrix(this.colNames, null);

        // The projection along the directions in this.projectionMatrix.
        // The projected rows are rows in this matrix too.
        DoubleMatrix proj = mat.mmul(this.projectionMatrix.transpose());

        // Copy all existing columns to the column list for the new table.
        List<IColumn> columns = new ArrayList<IColumn>();
        Iterable<IColumn> inputColumns = data.getColumns();
        for (IColumn inputColumn : inputColumns) {
            columns.add(inputColumn);
        }

        // Add all the projections to the columns.
        for (int j = 0; j < this.numLowDims; j++) {
            String colName = String.format("LinearProjection%d", j);
            ColumnDescription colDesc = new ColumnDescription(colName, ContentsKind.Double, false);
            DoubleArrayColumn column = new DoubleArrayColumn(colDesc, data.getMembershipSet().getSize());
            IRowIterator it = data.getMembershipSet().getIterator();
            int row = it.getNextRow();
            int i = 0;
            while (row >= 0) {
                column.set(row, proj.get(i, j));
                row = it.getNextRow();
                i++;
            }
            columns.add(column);
        }

        return new Table(columns, data.getMembershipSet());
    }
}

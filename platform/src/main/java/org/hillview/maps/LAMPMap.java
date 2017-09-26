package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.ColumnDescription;

import org.hillview.table.Table;
import org.hillview.table.api.*;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.SparseColumn;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.jblas.Singular;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LAMPMap implements IMap<ITable, ITable> {
    private static double eps = 1e-9;
    private final List<String> numColNames;
    private final DoubleMatrix highDimControlPoints;
    private final DoubleMatrix lowDimControlPoints;
    private final int highDims;
    private final int lowDims;
    private final List<String> newColNames;

    public LAMPMap(DoubleMatrix highDimControlPoints, DoubleMatrix lowDimControlPoints, List<String> numColNames,
                   List<String> newColNames) {
        this.numColNames = numColNames;
        this.highDimControlPoints = highDimControlPoints;
        this.lowDimControlPoints = lowDimControlPoints;
        this.lowDims = lowDimControlPoints.columns;
        this.highDims = highDimControlPoints.columns;
        this.newColNames = newColNames;
    }

    @Override
    public ITable apply(ITable data) {
        List<IColumn> columns = numColNames.stream().map(data::getColumn).collect(Collectors.toList());

        List<IMutableColumn> newColumns = new ArrayList<IMutableColumn>(this.lowDims);
        IMembershipSet set = data.getMembershipSet();
        int colSize = set.getMax();
        for (int i = 0; i < this.lowDims; i++) {
            ColumnDescription cd = new ColumnDescription(this.newColNames.get(i), ContentsKind.Double, true);
            if (set.useSparseColumn(set.getSize()))
                newColumns.add(new SparseColumn(cd, colSize));
            else
                newColumns.add(new DoubleArrayColumn(cd, colSize));
        }

        IRowIterator rowIt = data.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            int finalRow = row;
            double[] rowData = columns.stream().map((col) -> col.asDouble(finalRow, null)).mapToDouble(v -> v)
                    .toArray();
            DoubleMatrix x = new DoubleMatrix(rowData).reshape(1, this.highDims);
            DoubleMatrix y = computeMapping(x);
            if (y.isNaN().sum() > 0) {
                for (int i = 0; i < this.lowDims; i++) {
                    newColumns.get(i).setMissing(row);
                }
            } else {
                for (int i = 0; i < this.lowDims; i++) {
                    newColumns.get(i).set(row, y.get(i));
                }
            }
            row = rowIt.getNextRow();
        }

        List<IColumn> allColumns = new ArrayList<IColumn>();
        /*Only add the original columns that were not already this map's new names.*/
        /*This means that those columns are replaced if they're there!*/
        data.getColumns().forEach((col) -> {
            for (String newColName : this.newColNames) {
                if (col.getName() == newColName)
                    return;
            }
            allColumns.add(col);
        });
        allColumns.addAll(newColumns);
        return new Table(allColumns);
    }

    private DoubleMatrix computeMapping(DoubleMatrix x) {
        DoubleMatrix alphas = MatrixFunctions.pow(
                MatrixFunctions.pow(
                        this.highDimControlPoints.subRowVector(x),
                        2
                ).rowSums(),
                -1
        );
        double alpha = alphas.sum();

        DoubleMatrix xTilde = this.highDimControlPoints.mulColumnVector(alphas).columnSums().div(alpha);
        DoubleMatrix yTilde = this.lowDimControlPoints.mulColumnVector(alphas).columnSums().div(alpha);
        DoubleMatrix xHats = this.highDimControlPoints.subRowVector(xTilde);
        DoubleMatrix yHats = this.lowDimControlPoints.subRowVector(yTilde);

        DoubleMatrix sqrtAlphas = MatrixFunctions.sqrt(alphas);
        DoubleMatrix A = xHats.mulColumnVector(sqrtAlphas);
        DoubleMatrix B = yHats.mulColumnVector(sqrtAlphas);

        DoubleMatrix[] svdComposition = Singular.sparseSVD(A.transpose().mmul(B));
        DoubleMatrix U = svdComposition[0];
        DoubleMatrix V = svdComposition[2];

        DoubleMatrix M = U.mmul(V);

        return x.sub(xTilde).mmul(M).add(yTilde);
    }
}
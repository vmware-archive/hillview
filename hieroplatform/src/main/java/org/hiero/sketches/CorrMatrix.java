package org.hiero.sketches;

import java.util.Arrays;
import java.util.List;

/**
 * This matrix is used in computing the inner products between a list of columns. It implements the
 * ICorrelation interface to compute  norms, correlations and inner-products between columns. See
 * ICorrelation for a precise definition of these quantities.
 */
public class CorrMatrix implements ICorrelation {
    /**
     * The list of columns whose correlation we wish to compute.
     */
    private final List<String> colNames;
    /**
     * A matrix that records the un-normalized inner products between pairs of columns.
     */
    private final double[][] matrix;
    /**
     * A count of the number of entries processed so far.
     */
    public long count;

    public CorrMatrix(List<String> colNames) {
        this.colNames = colNames;
        this.matrix = new double[colNames.size()][colNames.size()];
        this.count = 0;
    }

    public void update(int i, int j, double val) {
        this.matrix[i][j] += val;
    }

    public double get(int i, int j) {
        return this.matrix[i][j];
    }

    @Override
    public double getCorrelation(String s, String t) {
        int i = this.colNames.indexOf(s);
        int j = this.colNames.indexOf(t);
        double val = (i <= j) ? this.matrix[i][j] : this.matrix[j][i];
        return val/Math.sqrt(this.matrix[i][i]*this.matrix[j][j]);
    }

    @Override
    public double[][] getCorrelationMatrix() {
        double[][] m = new double[this.colNames.size()][this.colNames.size()];
        for (int i = 0; i < this.colNames.size(); i++)
            for (int j = i; j < this.colNames.size(); j++) {
                m[i][j] = this.matrix[i][j] / (Math.sqrt(this.matrix[i][i] * this.matrix[j][j]));
                m[j][i] = m[i][j];
            }
        return m;
    }

    @Override
    public double getNorm(String s) {
        return Math.sqrt(this.matrix[this.colNames.indexOf(s)][this.colNames.indexOf(s)]/this.count);
    }

    @Override
    public double getInnerProduct(String s, String t) {
        int i = this.colNames.indexOf(s);
        int j = this.colNames.indexOf(t);
        return (((i <= j) ? this.matrix[i][j] : this.matrix[j][i])/this.count);
    }

    @Override
    public double[] getCorrelationWith(String s) {
        return getCorrelationMatrix()[this.colNames.indexOf(s)];
    }

    public String toString() {
        return "Number of columns:  " + String.valueOf(this.colNames.size()) + "\n" +
                "Total count: " + String.valueOf(this.count) + "\n" +
                Arrays.deepToString(this.matrix);
    }
}

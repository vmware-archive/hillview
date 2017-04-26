package org.hiero.sketches;

import java.util.Arrays;

public class CorrMatrix {
    private final int numCols;
    private final double[][] matrix;
    public long count;

    public CorrMatrix(int numCols) {
        this.matrix = new double[numCols][numCols];
        this.numCols = numCols;
        this.count = 0;
    }

    public void update(int i, int j, double val) {
        this.matrix[i][j] += val;
    }

    public double get(int i, int j) {
        return this.matrix[i][j];
    }

    public double getCorr(int i, int j) {
        return this.matrix[i][j]/this.count;
    }

    public double[][] getCorrMatrix() {
        double[][] m = new double[this.numCols][this.numCols];
        for (int i = 0; i < this.numCols; i++)
            for (int j =0; j < this.numCols; j++)
                m[i][j] = this.matrix[i][j]/(Math.sqrt(this.matrix[i][i]*this.matrix[j][j]));
        return m;
    }

    public String toString() {
        String sb = "Number of columns:  " + String.valueOf(this.numCols) + "\n" +
                "Total count: " + String.valueOf(this.count) + "\n" +
                Arrays.deepToString(this.matrix);
        return sb;
    }
}

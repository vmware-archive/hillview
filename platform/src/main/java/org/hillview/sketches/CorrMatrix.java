package org.hillview.sketches;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * This rawMatrix is used in computing the inner products between a list of columns. It implements
 * the ICorrelation interface to compute  norms, correlations and inner-products between columns.
 * See ICorrelation for a precise definition of these quantities.
 */
public class CorrMatrix implements ICorrelation {
    /**
     * The list of columns whose correlation we wish to compute.
     */
    private final HashMap<String, Integer> colNum;
    /**
     * A matrix that records the un-normalized inner products between pairs of columns.
     */
    private final double[][] rawMatrix;
    /**
    * A matrix that computes correlations between pairs of columns.
    */
    @Nullable
    private double[][] corrMatrix;
    /**
     * A count of the number of entries processed so far.
     */
    public long count;

    public CorrMatrix(List<String> colNames) {
        this.colNum = new HashMap<>(colNames.size());
        for (int i=0; i < colNames.size(); i++)
            this.colNum.put(colNames.get(i), i);
        this.rawMatrix = new double[colNames.size()][colNames.size()];
        this.count = 0;
        this.corrMatrix = null;
    }

    public void update(int i, int j, double val) {
        this.rawMatrix[i][j] += val;
    }

    /**
     * Sets a new value at (i, j) that is a weighted sum of this matrix's value and the other one, weighted by the
     * number of elements processed for both matrices.
     */
    public void updateWeighted(int i, int j, double val, long otherCount) {
        long countSum = this.count + otherCount;
        double thisWeight = ((double) this.count) / countSum;
        double otherWeight = ((double) otherCount) / countSum;
        this.rawMatrix[i][j] = thisWeight * this.rawMatrix[i][j] + otherWeight * val;
    }

    public double get(int i, int j) {
        return this.rawMatrix[i][j];
    }

    @Override
    public double[][] getCorrelationMatrix() {
        if (this.corrMatrix == null) {
            this.corrMatrix = new double[this.colNum.size()][this.colNum.size()];
            for (int i = 0; i < this.colNum.size(); i++)
                for (int j = i; j < this.colNum.size(); j++) {
                    this.corrMatrix[i][j] = this.rawMatrix[i][j] / (Math.sqrt(this.rawMatrix[i][i] * this.rawMatrix[j][j]));
                    this.corrMatrix[j][i] = this.corrMatrix[i][j];
                }
        }
        return this.corrMatrix;
    }

    @Override
    public double[] getCorrelationWith(String s) {
            return this.getCorrelationMatrix()[this.colNum.get(s)];
    }

    @Override
    public double getCorrelation(String s, String t) {
        int i = this.colNum.get(s);
        int j = this.colNum.get(t);
        return this.getCorrelationMatrix()[i][j];
    }

    @Override
    public double getNorm(String s) {
        return Math.sqrt(this.rawMatrix[this.colNum.get(s)][this.colNum.get(s)]/this.count);
    }

    @Override
    public double getInnerProduct(String s, String t) {
        int i = this.colNum.get(s);
        int j = this.colNum.get(t);
        return (((i <= j) ? this.rawMatrix[i][j] : this.rawMatrix[j][i])/this.count);
    }


    public String toString() {
        return "Number of columns:  " + String.valueOf(this.colNum.size()) + "\n" +
                "Total count: " + String.valueOf(this.count) + "\n" +
                Arrays.deepToString(this.rawMatrix);
    }
}
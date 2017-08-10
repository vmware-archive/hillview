package org.hillview.sketches;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * This rawMatrix is used in computing the inner products between a list of columns. It implements
 * the ICorrelation interface to compute  norms, correlations and inner-products between columns.
 * See ICorrelation for a precise definition of these quantities.
 */
public class CorrMatrix implements ICorrelation, Serializable {
    /**
     * The list of columns whose correlation we wish to compute.
     */
    private final HashMap<String, Integer> colNum;
    /**
     * A matrix that records the (un)-normalized inner products between pairs of columns.
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
    /**
     * The means of the columns
     */
    public final double[] means;

    public CorrMatrix(List<String> colNames) {
        this.colNum = new HashMap<>(colNames.size());
        for (int i=0; i < colNames.size(); i++)
            this.colNum.put(colNames.get(i), i);
        this.rawMatrix = new double[colNames.size()][colNames.size()];
        this.count = 0;
        this.means = new double[colNames.size()];
        this.corrMatrix = null;
    }

    public void update(int i, int j, double val) {
        this.rawMatrix[i][j] += val;
    }

    public void put(int i, int j, double val) {
        this.rawMatrix[i][j] = val;
    }

    public double get(int i, int j) {
        return this.rawMatrix[i][j];
    }

    @Override
    public double[][] getCorrelationMatrix() {
        if (this.corrMatrix == null) {
            this.corrMatrix = new double[this.colNum.size()][this.colNum.size()];
            for (int i = 0; i < this.colNum.size(); i++) {
                double sigmaI = Math.sqrt(this.rawMatrix[i][i] - this.means[i] * this.means[i]);
                for (int j = i; j < this.colNum.size(); j++) {
                    double val = this.rawMatrix[i][j];
                    // Centering and scaling
                    val -= this.means[i] * this.means[j];
                    double sigmaJ = Math.sqrt(this.rawMatrix[j][j] - this.means[j] * this.means[j]);
                    val /= sigmaI * sigmaJ;
                    this.corrMatrix[i][j] = val;
                    this.corrMatrix[j][i] = val;
                }
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
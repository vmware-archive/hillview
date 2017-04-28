package org.hiero.sketches;

import java.security.InvalidParameterException;
import java.util.LinkedHashMap;
import java.util.List;

/** Data structure used to store the results of JL sketch.
 * It contains a vector fo doubles for each column, and some other information that can be used for
 * normalization. It implements the ICorrelation interface and can be used for computing norms,
 * inner products etc, but it is currently rather slow compared to smapling based methods.
 */
public class JLProjection implements ICorrelation {
    private final LinkedHashMap<String, double[]> hMap;
    private final int lowDim;
    public int highDim;
    public final List<String> colNames;

    public JLProjection(List<String> colNames, int lowDim) {
        this.lowDim = lowDim;
        this.colNames = colNames;
        this.hMap = new LinkedHashMap<String, double[]>();
        colNames.forEach(s -> this.hMap.put(s, new double[this.lowDim]));
        this.highDim = 0;
    }

    public void update(String s, int i, double val) {
        this.hMap.get(s)[i] += val;
    }

    public double get(String s, int i) {
        return this.hMap.get(s)[i];
    }

    public void scale(double f) {
        for (String s: this.colNames)
            for (int i = 0; i < this.lowDim; i++)
                this.hMap.get(s)[i] *= f;
    }

    public double getNorm(String s) {
        if (!this.colNames.contains(s))
            throw new InvalidParameterException("No sketch found for column: " + s);
        double sum = 0;
        for (int i = 0; i < this.lowDim; i++) {
            sum += Math.pow(this.get(s, i), 2);
        }
        return Math.sqrt(sum/(this.lowDim * this.highDim));
    }

    public double getInnerProduct(String s, String t) {
        if (!this.colNames.contains(s))
            throw new InvalidParameterException("No sketch found for column: " + s);
        if (!this.colNames.contains(t))
            throw new InvalidParameterException("No sketch found for column: " + t);
        double sum = 0;
        for (int i = 0; i < this.lowDim; i++) {
            sum += this.get(s, i)*this.get(t, i);
        }
        return (sum/(this.lowDim * this.highDim));
    }

    public double getCorrelation(String s, String t) {
        if (!this.colNames.contains(s))
            throw new InvalidParameterException("No sketch found for column: " + s);
        if (!this.colNames.contains(t))
            throw new InvalidParameterException("No sketch found for column: " + t);
        double sum =0, first =0, second =0;
        for (int i = 0; i < this.lowDim; i++) {
            sum += this.get(s, i)*this.get(t, i);
            first += Math.pow(this.get(s, i), 2);
            second += Math.pow(this.get(t, i), 2);
        }
        return (sum/Math.sqrt(first*second));
    }

    @Override
    public double[] getCorrelationWith(String s) {
        int d = this.colNames.size();
        double[] corrWith = new double[d];
        for (int i = 0; i < d; i++)
                corrWith[i] = this.getCorrelation(s, this.colNames.get(i));
        return corrWith;
    }

    @Override
    public double[][] getCorrelationMatrix() {
        int d = this.colNames.size();
        double[][] corr = new double[d][d];
        for (int i = 0; i < d; i++)
            for (int j = i; j < d; j++) {
                corr[i][j] = this.getCorrelation(this.colNames.get(i), this.colNames.get(j));
                corr[j][i] = corr[i][j];
        }
        return corr;
    }
}
/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.sketches.results;

import org.apache.commons.lang.ArrayUtils;
import org.hillview.sketches.results.ICorrelation;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.LinkedHashMap;

/** Data structure used to store the results of a Johnson-Lindenstrauss (JL) sketch.
 * It contains a vector of doubles for each column, and some other information that can be used for
 * normalization. It implements the ICorrelation interface and can be used for computing norms,
 * inner products etc, but it is currently rather slow compared to sampling based methods.
 */
public class JLProjection implements ICorrelation {
    /**
     * The JL Sketch stores for every column, a vector of doubles of dimension lowDim, which is the
     * sketch of that column. The sketch is computed by multiplying th column (viewed as a vector of
     * doubles) by a random matrix of {-1, 1} values.
     */
    public final LinkedHashMap<String, double[]> hMap;
    /**
     * The dimension we are projecting down to. The error in estimating the norm is proportional to
     * 1/sqrt(lowDim).
     */
    private final int lowDim;
    /**
     * The length of each column which is the dimension of each column vector.
     */
    public int highDim;
    /**
     * The list of columns we want to sketch. Each column should be of type Int/Double.
     */
    public final String[] colNames;
    /**
     * The matrix of pairwise correlations (see ICorrelation for exact definition of correlation).
     */
    @Nullable
    private double[][] corrMatrix;

    public JLProjection(String[] colNames, int lowDim) {
        if (lowDim <= 0)
            throw new InvalidParameterException("LowDim has to be positive.");
        this.lowDim = lowDim;
        this.colNames = colNames;
        this.hMap = new LinkedHashMap<String, double[]>();
        for (String s: colNames)
            this.hMap.put(s, new double[this.lowDim]);
        this.highDim = 0;
        this.corrMatrix = null;
    }

    public void update(String s, int j, double val) {
        this.hMap.get(s)[j] += val;
    }

    public double get(String s, int j) {
        return this.hMap.get(s)[j];
    }

    public void scale(double f) {
        for (String s: this.hMap.keySet()) {
            for (int i = 0; i < this.lowDim; i++)
                this.hMap.get(s)[i] *= f;
        }
    }

    public double getNorm(String s) {
        if (!this.hMap.containsKey(s))
            throw new InvalidParameterException("No sketch found for column: " + s);
        if (this.highDim <= 0)
            throw new InvalidParameterException("Dimension must be positive.");
        double sum = 0;
        double[] a = this.hMap.get(s);
        for (int i = 0; i < this.lowDim; i++)
            sum += Math.pow(a[i], 2);
        return Math.sqrt(sum/(this.lowDim * this.highDim));
    }

    public double getInnerProduct(String s, String t) {
        if (!this.hMap.containsKey(s))
            throw new InvalidParameterException("No sketch found for column: " + s);
        if (!this.hMap.containsKey(t))
            throw new InvalidParameterException("No sketch found for column: " + t);
        if (this.highDim <= 0)
            throw new InvalidParameterException("Dimension must be positive.");
        double sum = 0;
        double[] a = this.hMap.get(s);
        double[] b = this.hMap.get(t);
        for (int i = 0; i < this.lowDim; i++)
            sum += a[i]*b[i];
        return (sum/(this.lowDim * this.highDim));
    }

    @Override
    public double[][] getCorrelationMatrix() {
        if (this.corrMatrix == null) {
            int d = this.colNames.length;
            this.corrMatrix = new double[d][d];
            for (int i = 0; i < d; i++)
                for (int j = i; j < d; j++) {
                    double sum = 0, first = 0, second = 0;
                    double[] a = this.hMap.get(this.colNames[i]);
                    double[] b = this.hMap.get(this.colNames[j]);
                    for(int k = 0; k < this.lowDim; k++) {
                        sum += a[k]*b[k];
                        first += Math.pow(a[k], 2);
                        second += Math.pow(b[k], 2);
                    }
                    if ((first == 0) || (second == 0))
                        this.corrMatrix[i][j] = 0;
                    else
                        this.corrMatrix[i][j] = sum/Math.sqrt(first*second);
                    this.corrMatrix[j][i] = this.corrMatrix[i][j];
                }
        }
        return this.corrMatrix;
    }

    @Override
    public double getCorrelation(String s, String t) {
        int sIndex = ArrayUtils.indexOf(this.colNames, s);
        int tIndex = ArrayUtils.indexOf(this.colNames, t);
        return this.getCorrelationMatrix()[sIndex][tIndex];
    }

    @Override
    public double[] getCorrelationWith(String s) {
        int sIndex = ArrayUtils.indexOf(this.colNames, s);
        return this.getCorrelationMatrix()[sIndex];
    }
}

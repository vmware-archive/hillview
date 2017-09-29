/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;
import org.jblas.ranges.PointRange;
import org.jblas.ranges.AllRange;

import java.util.function.BiFunction;
import java.util.logging.Logger;

/**
 * This class optimizes the squared stress function with respect to a low-dimensional embedding, which is an objective
 * function that tries to preserve distances from the high-dimensional data set in a low-dimensional embedding.
 *
 * It can be used for finding (for example) 2D coordinates for entities in a dataset, if only the distances between
 * those entities are known (by some distance function).
 *
 * For more information: https://en.wikipedia.org/wiki/Multidimensional_scaling
 */
public class MetricMDS {
    private static final Logger LOG = Logger.getLogger(MetricMDS.class.getName());
    public static int maxIterations = (int) 1e3;
    public static final double defaultLearningRate = 1;
    public static final double defaultLearningRateDecay = 0.999;
    public static double tolerance = 1e-5;
    private static final double eps = 1e-9;
    public static final BiFunction<DoubleMatrix, DoubleMatrix, Double> squaredEuclid = (x1, x2) -> MatrixFunctions.pow(x1.sub(x2),2).sum();
    public static final BiFunction<DoubleMatrix, DoubleMatrix, Double> euclid = (x1, x2) -> Math.sqrt(MetricMDS.squaredEuclid.apply(x1, x2));

    /**
     * Number of observations in the dataset.
     */
    private int numObservations;
    /**
     * Number of output dimensions
     */
    private int lowDims;
    /**
     * Learning rate to use in the optimization. It is decreased over time to find a more accurate minimum.
     */
    public double learningRate = MetricMDS.defaultLearningRate;
    public double learningRateDecay = MetricMDS.defaultLearningRateDecay;
    /**
     * If the magnitude of the gradient is smaller than this value, we consider the optimization converged.
     */
    public double stopTolerance = MetricMDS.tolerance;

    /**
     * All pairwise distances d(i, j) in nD. Since it is symmetric, only the upper-triangular part is stored.
     * It is indexed as follows: d(i, j) = d(j, i) = distsHighDim[i * (N - (i + 3) / 2) + j - 1], with i < j < N, and N the
     * number of observations.
     * Note that the diagonal d(i, i) is not contained in the matrix, as d(i, i) = 0 always.
     */
    private final DoubleMatrix distsHighDim;
    /**
     * Normalization factor for the high-dimensional distances. This is later used to rescale the low-dimensional
     * points to reflect the original distances.
     */
    private double scaling;
    /**
     * Same format for the low-dimensional distances, but this matrix is recomputed every epoch.
     */
    private final DoubleMatrix distsLowDim;

    /**
     * The low-dimensional embedding of the high-dimensional data.
     */
    private final DoubleMatrix dataLowDim;

    private boolean verbose = false;

    /**
     * Constructs an object that calculates the metric MDS projection. Note that the low-dimensional distance metric is
     * always the Euclidean distance, as the gradient is calculated for this.
     * @param dataHighDim High-dimensional data with observations/{data points} as rows, and dimensions/features as columns.
     * @param lowDims The target dimensionality of the embedding. Commonly 2.
     * @param highDimDist The distance function for nD observations.
     */
    public MetricMDS(DoubleMatrix dataHighDim, int lowDims, BiFunction<DoubleMatrix, DoubleMatrix, Double> highDimDist) {
        this.numObservations = dataHighDim.rows;
        this.lowDims = lowDims;
        this.distsHighDim = this.computeHighDimDistances(dataHighDim, highDimDist);
        this.dataLowDim = new DoubleMatrix();
        this.distsLowDim = new DoubleMatrix();
    }

    public MetricMDS(DoubleMatrix dataHighDim, int lowDims) {
        this(dataHighDim, lowDims, MetricMDS.euclid);
    }

    public MetricMDS(DoubleMatrix dataHighDim) {
        this(dataHighDim, 2);
    }

    private int compactIndex(int i, int j) {
        return i * this.numObservations - (i * (i + 3)) / 2 + j - 1;
    }

    private DoubleMatrix computeHighDimDistances(DoubleMatrix dataHighDim, BiFunction<DoubleMatrix, DoubleMatrix, Double> distHighDim) {
        DoubleMatrix dists = new DoubleMatrix((dataHighDim.rows * (dataHighDim.rows - 1)) / 2);
        for (int i = 0; i < dataHighDim.rows - 1; i++) {
            DoubleMatrix x1 = dataHighDim.get(new PointRange(i), new AllRange());
            for (int j = i + 1; j < dataHighDim.rows; j++) {
                DoubleMatrix x2 = dataHighDim.get(new PointRange(j), new AllRange());
                double dist = distHighDim.apply(x1, x2);
                int idx = this.compactIndex(i, j);
                dists.put(idx, dist);
            }
        }
        /* Normalize the distances s.t. the largest is 1. */
        this.scaling = 1 / dists.max();
        dists.muli(this.scaling);

        return dists;
    }

    private DoubleMatrix computeLowDimDistances(DoubleMatrix dataLowDim) {
        DoubleMatrix dists = new DoubleMatrix((dataLowDim.rows * (dataLowDim.rows - 1)) / 2);
        for (int i = 0; i < dataLowDim.rows - 1; i++) {
            DoubleMatrix x1 = dataLowDim.get(new PointRange(i), new AllRange());
            for (int j = i + 1; j < dataLowDim.rows; j++) {
                DoubleMatrix x2 = dataLowDim.get(new PointRange(j), new AllRange());
                double dist = MetricMDS.euclid.apply(x1, x2);
                int idx = this.compactIndex(i, j);
                dists.put(idx, dist);
            }
        }
        return dists;
    }

    /**
     * Auxiliary for accessing distances in a compact matrix.
     * @param compactMatrix Distance matrix, stored compactly, as described in [MetricMds].
     * @param i Observation index
     * @param j Observation index
     * @return The distances between the high-dimensional observations.
     */
    private double getDist(DoubleMatrix compactMatrix, int i, int j) {
        if (i < j)
            return compactMatrix.get(this.compactIndex(i, j));
        else if (j < i) {
            return compactMatrix.get(this.compactIndex(j, i));
        } else {
            return 0;
        }
    }

    /**
     * Compute a projection to this.lowDims dimensions. The initial guess for the projection is set to the given matrix.
     * @param dataLowDimInit Initial value for the projection.
     * @return Projection of the high-dimensional data, computed with metric mds.
     */
    public DoubleMatrix computeEmbedding(DoubleMatrix dataLowDimInit) {
        this.dataLowDim.copy(dataLowDimInit);
        this.distsLowDim.copy(this.computeLowDimDistances(this.dataLowDim));

        int iterations = 0;
        double cost = this.cost();
        double magnitude;
        double initialCost = cost;
        int convergedCount = 0;
        do {
            /* Compute the gradient */
            DoubleMatrix gradient = this.gradient();
            DoubleMatrix step = this.gradient().mul(this.learningRate / this.numObservations).neg();
            /* Move the low-dimensional points s.t. the cost locally decreases. */
            this.dataLowDim.addi(step);

            magnitude = gradient.norm2() / this.numObservations;
            double newCost = this.cost();
            if (this.verbose) {
                LOG.info(
                    String.format(
                        "\n[Iteration %d]\n\tCost:        \t%6.3e\n\tMagnitude:   \t%6.3e\n\tLearning rate:\t%6.3e",
                        iterations,
                        newCost,
                        magnitude,
                        learningRate
                    )
                );
            }

            this.learningRate *= this.learningRateDecay;
            iterations++;
            cost = newCost;
            /* Continue while the convergence criterion isn't met and the max # iterations isn't met. */
        } while (Math.abs(magnitude) > MetricMDS.tolerance && iterations < MetricMDS.maxIterations);

        if (Math.abs(magnitude) > MetricMDS.tolerance)
            LOG.warning("Terminated before tolerance was met.");
        LOG.info(String.format("MDS optimization took %d iterations.", iterations));
        LOG.info(String.format("\nCost before optimization: %6.3e\nCost after optimization:  %6.3e", initialCost,
                cost));

        /* Divide by the normalization factor, s.t. the result reflects the original distances. */
        return this.dataLowDim.div(this.scaling);
    }

    /**
     * Compute a projection to this.lowDims dimensions. The initial guess for the projection is set with the given
     * random seed.
     * @param seed Random seed for the initial projection.
     * @return Projection of the high-dimensional data, computed with metric mds.
     */
    public DoubleMatrix computeEmbedding(long seed) {
        DoubleMatrix dataLowDimInit = new DoubleMatrix(this.numObservations, this.lowDims);
        Randomness rnd = new Randomness(seed);
        rnd.nextGaussian();
        for (int i = 0; i < this.numObservations; i++) {
            for (int j = 0; j < this.lowDims; j++) {
                dataLowDimInit.put(i, j, rnd.nextGaussian());
            }
        }
        return this.computeEmbedding(dataLowDimInit);
    }

    /**
     * Compute the gradient.
     * @return The gradient of the cost function w.r.t. the low-dimensional points.
     */
    private DoubleMatrix gradient() {
        DoubleMatrix gradient = DoubleMatrix.zeros(this.numObservations, this.lowDims);
        this.distsLowDim.copy(this.computeLowDimDistances(this.dataLowDim));

        /* Loop over all pairs of points. */
        for (int i = 0; i < this.numObservations - 1; i++) {
            DoubleMatrix pointI = this.dataLowDim.getRow(i);
            for (int j = i + 1; j < this.numObservations; j++) {
                DoubleMatrix pointJ = this.dataLowDim.getRow(j);
                /* Compute gradient for point i (only w.r.t. points with index > i) */

                /* Vector from (low-dim) point i to point j */
                DoubleMatrix gradientI = pointJ.sub(pointI);
                /* Make it a unit vector */
                gradientI.divi(Math.max(this.getDist(this.distsLowDim, j, i), MetricMDS.eps));
                /* Scale by the discrepancy */
                gradientI.muli(this.getDist(this.distsHighDim, j, i) - this.getDist(this.distsLowDim, j, i));

                /* Exploit symmetry: The gradient on i caused by j is equal to the inverse gradient on j caused by i. */
                gradient.putRow(i, gradient.getRow(i).add(gradientI));
                gradient.putRow(j, gradient.getRow(j).add(gradientI.neg()));
            }
        }

        gradient.muli(2);

        return gradient;
    }

    public double cost() {
        return MatrixFunctions.pow(this.distsHighDim.sub(this.distsLowDim), 2).sum();
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
}

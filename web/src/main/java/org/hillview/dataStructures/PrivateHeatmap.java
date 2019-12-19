package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Heatmap;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PrivateHeatmap implements Serializable, IJson {
    public Heatmap heatmap;
    private double epsilon;
    private SecureLaplace laplace;
    static final boolean coarsen = false;

    public PrivateHeatmap(IntervalDecomposition d0, IntervalDecomposition d1,
                          Heatmap heatmap, double epsilon, SecureLaplace laplace) {
        this.heatmap = heatmap;
        this.epsilon = epsilon;
        this.laplace = laplace;
        this.addDyadicLaplaceNoise(d0, d1);
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * If cdfBuckets is true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     * rather than [bucket left leaf, bucket right leaf].
     * Returns the noise and the total variance of the variables used to compute the noise.
     */
    private void noiseForBucket(
            List<Pair<Integer, Integer>> xIntervals,
            List<Pair<Integer, Integer>> yIntervals,
            double scale,
            double baseVariance,
            /*out*/Noise result) {
        result.clear();
        for (Pair<Integer, Integer> x : xIntervals) {
            for (Pair<Integer, Integer> y : yIntervals) {
                result.noise += this.laplace.sampleLaplace(x, y, scale);
                result.variance += baseVariance;
            }
        }
    }

    private static boolean notConfident(long value, int noise) {
        return value < noise;
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    private void addDyadicLaplaceNoise(IntervalDecomposition dx, IntervalDecomposition dy) {
        HillviewLogger.instance.info("Adding heatmap noise with", "epsilon={0}", this.epsilon);
        int xSize = this.heatmap.xBucketCount;
        int ySize = this.heatmap.yBucketCount;
        List<List<Pair<Integer, Integer>>> xIntervals = new ArrayList<List<Pair<Integer, Integer>>>(xSize);
        List<List<Pair<Integer, Integer>>> yIntervals = new ArrayList<List<Pair<Integer, Integer>>>(ySize);
        for (int i = 0; i < xSize; i++)
            xIntervals.add(dx.bucketDecomposition(i, false));
        for (int i = 0; i < ySize; i++)
            yIntervals.add(dy.bucketDecomposition(i, false));

        Noise noise = new Noise();
        this.heatmap.allocateConfidence();
        long totalLeaves = (1 + dx.getQuantizationIntervalCount()) *
            (1 + dy.getQuantizationIntervalCount());  // +1 for the NULL bucket
        double scale = Math.log(totalLeaves) / Math.log(2);
        scale /= epsilon;
        double baseVariance = 2 * (Math.pow(scale, 2));
        Converters.checkNull(this.heatmap.confidence);

        boolean[][] noisy = new boolean[xSize][ySize];
        for (int i = 0; i < this.heatmap.buckets.length; i++) {
            for (int j = 0; j < this.heatmap.buckets[i].length; j++) {
                this.noiseForBucket(xIntervals.get(i), yIntervals.get(j), scale, baseVariance, noise);
                this.heatmap.buckets[i][j] += noise.noise;
                this.heatmap.confidence[i][j] = (int)(2 * Math.sqrt(noise.variance));
                noisy[i][j] = notConfident(this.heatmap.buckets[i][j], this.heatmap.confidence[i][j]);
            }
        }

        if (!coarsen)
            return;
        
        /* if some buckets are too noisy try to merge them with their neighbors */
        int xRect = 1; // x size of merged rectangles
        int yRect = 1; // y size of merged rectangles
        boolean done = false;
        while (!done) {
            done = true;
            if (xSize > 2 * xRect) {
                xRect *= 2;
                dx = dx.mergeNeighbors();
                xIntervals.clear();
                for (int i = 0; i < xSize / xRect; i++)
                    xIntervals.add(dx.bucketDecomposition(i, false));
                for (int y = 0; y < ySize; y += yRect) {
                    for (int x = 0; x < xSize; x += xRect) {
                        if (x + 2 * xRect < xSize && noisy[x][y] && noisy[x + xRect / 2][y]) {
                            noisy[x][y] = this.average(x, xRect, y, yRect, xIntervals, yIntervals,
                                    scale, baseVariance, noise);
                            done = false;
                        } else {
                            noisy[x][y] = false;  // cannot merge this rectangle - we are done with it.
                        }
                    }
                }
            }
            if (ySize > 2 * yRect) {
                yRect *= 2;
                dy = dy.mergeNeighbors();
                yIntervals.clear();
                for (int i = 0; i < ySize / yRect; i++)
                    yIntervals.add(dy.bucketDecomposition(i, false));
                for (int y = 0; y < ySize; y += yRect) {
                    for (int x = 0; x < xSize; x += xRect) {
                        if (y + 2 * yRect < ySize && noisy[x][y] && noisy[x][y + yRect / 2]) {
                            noisy[x][y] = this.average(x, xRect, y, yRect, xIntervals, yIntervals,
                                    scale, baseVariance, noise);
                            done = false;
                        } else {
                            noisy[x][y] = false;
                        }
                    }
                }
            }
        }
    }

    /**
     * Average all values in the heatmap in the specified rectangle.
     * Compute a new confidence.  Return true if the value is still too noisy.
     */
    private boolean average(int xCorner, int xRect, int yCorner, int yRect,
                            List<List<Pair<Integer, Integer>>> xIntervals,
                            List<List<Pair<Integer, Integer>>> yIntervals,
                            double scale, double baseVariance, Noise noise) {
        double value = 0;
        for (int i = 0; i < xRect; i++) {
            for (int j = 0; j < yRect; j++) {
                value += this.heatmap.buckets[xCorner + i][yCorner + j];
            }
        }
        double average = value / (xRect * yRect);
        this.noiseForBucket(xIntervals.get(xCorner / xRect), yIntervals.get(yCorner / yRect), scale, baseVariance, noise);
        double confidence = 2 * Math.sqrt(noise.variance);
        Converters.checkNull(this.heatmap.confidence);
        for (int i = 0; i < xRect; i++) {
            for (int j = 0; j < yRect; j++) {
                this.heatmap.buckets[xCorner + i][yCorner + j] = (long)average;
                this.heatmap.confidence[i][j] = (int)confidence;
            }
        }
        return notConfident((long)average, (int)confidence);
    }
}

package org.hillview.utils;

import org.hillview.dataStructures.IntervalDecomposition;

public class PrivacyUtils {

    /**
     * Compute the noise scale for a k-dimensional interval decomposition with branching factor as defined in IntervalDecomposition.
     *
     * @param epsilon the total budget for the k-d histogram
     * @param decompositions the leaf specification for each tree
     * @return
     */
    public static double computeNoiseScale(double epsilon,
                                           IntervalDecomposition... decompositions) {
        double scale = 1;
        for (IntervalDecomposition x : decompositions) {
            int totalLeaves = x.getQuantizationIntervalCount();
            scale *= Math.ceil(Utilities.logb(totalLeaves, IntervalDecomposition.BRANCHING_FACTOR)); // Ceil for nearest power of b
        }
        scale /= epsilon;
        return scale;
    }

    public static double laplaceVariance(double scale) {
        return 2*Math.pow(scale, 2);
    }
}

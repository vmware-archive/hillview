package org.hillview.utils;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataStructures.IntervalDecomposition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

public class PrivacyUtils {

    /**
     * Compute the noise scale for a k-dimensional interval decomposition with branching factor as defined
     * in IntervalDecomposition.
     * @param epsilon the total budget for the k-d histogram
     * @param decompositions the leaf specification for each tree
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

    /**
     * Cache the confidence intervals as we will use the same CIs many times for a given table.
     * The map is {(numVariables, scale) : {alpha : (lower bound, upper bound)}}.
     */
    public static LinkedHashMap<Pair<Integer, Double>,
            LinkedHashMap<Double, Pair<Double, Double>>> CIMemo = new LinkedHashMap<>();

    /**
     * Number of samples to use in computing confidence intervals.
     * TODO: This should probably vary based on the number of variables.
     */
    private static final int CI_SAMPLES = 10000;

    /**
     * Default significance level to use in confidnece intervals.
     */
    public static final double DEFAULT_ALPHA = 0.01;

    /**
     * Sample the distribution of the sum of numVariables zero-centered IID Laplace random variables with scale scale
     * and compute the 1-alpha confidence interval.
     * Note that there is no need for these samples to be secure as the CI is added as postprocessing.
     * Memoized for performance.
     *
     * @param numVariables the number of variables to sum
     * @param scale the scale of the variables
     * @param alpha the significance level for the confidence interval
     * @return lower and upper bounds for the confidence interval
     */
    public static Pair<Double, Double> laplaceCI(long numVariables, double scale, double alpha) {
        LinkedHashMap<Double, Pair<Double, Double>> cis = CIMemo.get(
                new Pair<Integer, Double>((int)numVariables, scale));
        if (cis != null) {
            Pair<Double, Double> ret = cis.get(alpha);
            if (ret != null) {
                return ret;
            }
        }

        ArrayList<LaplaceDistribution> vars = new ArrayList<>();
        for (int i = 0; i < numVariables; i++) {
            vars.add(new LaplaceDistribution(0.0, scale));
        }

        double[] totals = new double[CI_SAMPLES];
        for (LaplaceDistribution var : vars) {
            double[] samples = var.sample(CI_SAMPLES);
            for (int i = 0; i < totals.length; i++) {
                totals[i] += samples[i];
            }
        }

        Arrays.sort(totals);
        int lowerIdx = (int)(CI_SAMPLES * alpha);
        int upperIdx = CI_SAMPLES - lowerIdx;
        Pair<Double, Double> ret = new Pair<Double, Double>(totals[lowerIdx], totals[upperIdx]);
        if (cis != null) {
            cis.put(alpha, ret);
        } else {
            CIMemo.put(new Pair<Integer, Double>((int)numVariables, scale),
                    new LinkedHashMap<Double, Pair<Double, Double>>());
            LinkedHashMap<Double, Pair<Double, Double>> newCIs =
                    CIMemo.get(new Pair<Integer, Double>((int)numVariables, scale));
            newCIs.put(alpha, ret);
        }

        return ret;
    }
}

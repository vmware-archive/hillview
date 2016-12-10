package org.hiero.sketch;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class PercentileTest {
    @Test
    public void plotPercentile() {
        final int resolution = 2000;
        final int invErrSq= 100;
        final int sampleSize = 5*invErrSq*resolution;
        final int range = 1000000;
        int i;
        int j;
        int runs;

        for (runs = 0; runs < 1; runs++) {
            final int[] percentile = new int[resolution];
            final Random rn = new Random();
            for (i = 0; i < sampleSize; i++) {
                j = (int) Math.floor((rn.nextInt(range) * resolution) / range);
                percentile[j]++;
            }
            /*
            for (j = 0; j < 100; j++)
            System.out.printf("Bucket: %d, Count: %d%n", j, percentile[j]);
            */
            Arrays.sort(percentile);
            /*
            System.out.printf("Min: %d, Mean: %d, Max: %d%n",
                    percentile[0], sampleSize / resolution, percentile[resolution -1]);
            */
        }
    }
}


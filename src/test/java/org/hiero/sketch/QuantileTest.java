package org.hiero.sketch;

import org.hiero.sketch.table.Quantiles;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class QuantileTest {

    @Test
    public void TestQOne() {
        final int inpSize = 100;
        final int resolution = 5;
        final int[] input = new int[inpSize];
        final Random rn = new Random();
        for (int i = 0; i < inpSize; i++) {
            input[i] = rn.nextInt(inpSize);
        }
        final Comparator<Integer> comp = MyCompare.instance;
        final Quantiles qn = new Quantiles(input, comp);
        @SuppressWarnings("UnusedAssignment") final Integer[] qtiles= qn.getQuantiles(resolution);
        Arrays.sort(input);
        for(int i =0; i < resolution; i++) {
            @SuppressWarnings("UnusedAssignment") final int j = (inpSize * i) / resolution;
            /*
            System.out.printf("Quantile %d:  %d (%d) %n", i, qtiles[i], qtiles[i] - input[j]);
             */
        }
    }

    @Test
    public void TestQTwo() {
        final int inpSize = 100;
        final int resolution = 10;
        final int[] input = new int[inpSize];
        final Random rn = new Random();
        for (int i = 0; i < inpSize; i++) {
            input[i] = rn.nextInt(inpSize);
        }
        final Comparator<Integer> comp = MyCompare.instance;
        final Quantiles qn = new Quantiles(input, comp);
        final Integer[] qtiles= qn.getQuantiles(resolution);
        Arrays.sort(input);
        int j =0;
        for(int i =0; i < resolution; i++) {
            while(input[j] < qtiles[i])
                j++;
            @SuppressWarnings("UnusedAssignment") final double k = ((float) j * resolution) / inpSize;
            /*
            System.out.printf("Quantile %d (%f) %n", i, k);
             */
        }
    }
}
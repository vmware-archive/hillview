package org.hiero.sketch;

import org.hiero.sketch.table.Quantiles;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class QuantileTest {


    @Test
    public void TestQOne() {
        int inpSize = 100;
        int resolution = 5;
        int[] input = new int[inpSize];
        Random rn = new Random();
        for (int i = 0; i < inpSize; i++) {
            input[i] = rn.nextInt(inpSize);
        }
        Comparator<Integer> comp = MyCompare.instance;
        Quantiles qn = new Quantiles(input, comp);
        Integer[] qtiles= qn.getQuantiles(resolution);
        Arrays.sort(input);
        for(int i =0; i < resolution; i++) {
            int j = inpSize*i/resolution;
            /*
            System.out.printf("Quantile %d:  %d (%d) %n", i, qtiles[i], qtiles[i] - input[j]);
             */
        }
    }

    @Test
    public void TestQTwo() {
        int inpSize = 100;
        int resolution = 10;
        int[] input = new int[inpSize];
        Random rn = new Random();
        for (int i = 0; i < inpSize; i++) {
            input[i] = rn.nextInt(inpSize);
        }
        Comparator<Integer> comp = MyCompare.instance;
        Quantiles qn = new Quantiles(input, comp);
        Integer[] qtiles= qn.getQuantiles(resolution);
        Arrays.sort(input);
        int j =0;
        for(int i =0; i < resolution; i++) {
            while(input[j] < qtiles[i])
                j++;
            double k = (float)j *resolution/inpSize;
            /*
            System.out.printf("Quantile %d (%f) %n", i, k);
             */
        }
    }
}

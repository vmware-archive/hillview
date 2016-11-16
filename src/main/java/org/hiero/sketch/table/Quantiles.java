package org.hiero.sketch.table;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class Quantiles {
    private final int inpSize;
    private final int [] inpArray;
    private final Comparator<Integer> comp;

    public Quantiles(final int [] input, final Comparator<Integer> comp){
        this.inpSize = input.length;
        this.inpArray = input;
        this.comp = comp;
    }

    public Integer[] getQuantiles(final int resolution){
        final int invError = 100;
        int sampleSize = invError * resolution;
        final Integer[] Sample = new Integer[sampleSize];
        final Random rn = new Random();
        for(int i = 0; i < sampleSize; i++){
            final int j = rn.nextInt(this.inpSize);
            Sample[i] = (this.inpArray[j]);
        }
        Arrays.sort(Sample, this.comp);
        final Integer [] qtiles = new Integer[resolution +1];
        for (int i=0; i < resolution; i++){
            qtiles[i] = Sample[i*invError];
        }
        qtiles[resolution] = Sample[sampleSize -1];
        return qtiles;
    }
}

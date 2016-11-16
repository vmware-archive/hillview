package org.hiero.sketch.table;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class Quantiles {
    private int inpSize;
    private int [] inpArray;
    private Comparator<Integer> comp;
    int sampleSize;

    public Quantiles(int [] input, Comparator<Integer> comp){
        this.inpSize = input.length;
        this.inpArray = input;
        this.comp = comp;
    }

    public Integer[] getQuantiles(int resolution){
        int invError = 100;
        sampleSize = invError*resolution;
        Integer[] Sample = new Integer[sampleSize];
        final Random rn = new Random();
        for(int i =0; i < sampleSize; i++){
            int j = rn.nextInt(inpSize);
            Sample[i] = (inpArray[j]);
        }
        Arrays.sort(Sample,comp);
        Integer [] qtiles = new Integer[resolution +1];
        for (int i=0; i < resolution; i++){
            qtiles[i] = Sample[i*invError];
        }
        qtiles[resolution] = Sample[sampleSize -1];
        return qtiles;
    }
}

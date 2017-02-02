package org.hiero.utils;

import org.apache.commons.math3.random.MersenneTwister;

public class Randomness {
    private static Randomness prg;
    final private MersenneTwister myPrg;

    private Randomness() {
        this.myPrg = new MersenneTwister();
    }

    private Randomness(long seed) {
        this.myPrg = new MersenneTwister(seed);
    }

    public static Randomness getInstance() {
        if (prg == null)
            prg = new Randomness();
        return prg;
    }

    public static Randomness getInstance(long seed) {
        if (prg == null)
            prg = new Randomness(seed);
        return prg;
    }

    public int nextInt() { return this.myPrg.nextInt(); }

    public int nextInt(int range) { return this.myPrg.nextInt(range); }

    public double nextDouble() { return this.myPrg.nextDouble(); }
}

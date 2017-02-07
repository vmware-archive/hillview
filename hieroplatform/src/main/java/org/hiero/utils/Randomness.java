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

    /**
     * @return If existing, the instance. Otherwise a new instance.
     */
    public static Randomness getInstance() {
        if (prg == null)
            prg = new Randomness();
        return prg;
    }

    /**
     * @return a new instance of Randomness, whether one existed before or not.
     */
    public static Randomness createInstance(long seed) {
        prg = new Randomness(seed);
        return prg;
    }

    public static Randomness createInstance() {
        prg = new Randomness();
        return prg;
    }

    public int nextInt() { return this.myPrg.nextInt(); }

    public int nextInt(int range) { return this.myPrg.nextInt(range); }

    public double nextDouble() { return this.myPrg.nextDouble(); }

    public void setSeed(long seed) { this.myPrg.setSeed(seed); }
}

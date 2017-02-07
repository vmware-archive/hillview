package org.hiero.sketch;

import org.apache.commons.math3.random.MersenneTwister;
import org.junit.Test;

import java.util.Random;


public class RandomnessTest {
    Random randomPRG;
    MersenneTwister MT_PRG;

    @Test
    public void testRandomnessPerf() {

        this.randomPRG = new Random();
        this.MT_PRG = new MersenneTwister();
        int iterationnNum = 1000; // number of iterations
        int length = 100000; // number of random numbers to generate

        TestUtil.runPerfTest((k) -> totalRandom(length), iterationnNum);
        TestUtil.runPerfTest((k) -> totalMT(length), iterationnNum);
    }

    private void totalRandom(int k) {
        for (int i = 0; i < k; i++)
            this.randomPRG.nextInt();
    }

    private void totalMT(int k) {
        for (int i = 0; i < k; i++)
            this.MT_PRG.nextInt();
    }
}
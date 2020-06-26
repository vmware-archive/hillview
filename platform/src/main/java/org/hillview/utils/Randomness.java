/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import org.apache.commons.math3.random.MersenneTwister;

@SuppressWarnings("unused")
public class Randomness {
    final private MersenneTwister myPrg;

    public Randomness(long seed) {
        this.myPrg = new MersenneTwister(seed);
    }

    public int nextInt() { return this.myPrg.nextInt(); }

    public int nextInt(int range) { return this.myPrg.nextInt(range); }

    public double nextDouble() { return this.myPrg.nextDouble(); }

    public boolean nextBoolean() { return this.myPrg.nextBoolean(); }

    /**
     * @return the next pseudorandom, Gaussian ("normally") distributed double value with mean 0.0
     * and standard deviation 1.0 from this random number generator's sequence.
     */
    public double nextGaussian() { return this.myPrg.nextGaussian(); }

    public long nextLong() { return this.myPrg.nextLong(); }

    /**
     * @return the next pseudorandom Geometric with distribution parameter p. Should be more
     * efficient than using logs.
     */
    public int nextGeometric(double p) {
        if (p <= 0) throw new IllegalArgumentException("parameter of Geometric Distribution must be positive");
        int value = myPrg.nextInt();
        double sample = (double) Math.abs(value) / Integer.MAX_VALUE;
        return Converters.toIntClamp(Math.floor(Math.log(1 - sample) / Math.log(1 - p)) + 1);
    }

    /**
     * returns a long uniformly drawn between 0 (inclusive) and n (exclusive)
      */
    public long nextLong(long n) { return this.myPrg.nextLong(n); }

    /**
     * Generates random bytes and places them into a user-supplied byte array. The number of
     * random bytes produced is equal to the length of the byte array.
     */
    public void nextBytes(byte[] bytes) { this.myPrg.nextBytes(bytes); }
}

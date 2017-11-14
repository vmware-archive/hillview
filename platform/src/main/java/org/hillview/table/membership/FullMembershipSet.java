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

package org.hillview.table.membership;

import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IMutableMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.utils.Randomness;

/**
 * A IMembershipSet which contains all rows.
 */
public class FullMembershipSet implements IMembershipSet {
    private final int rowCount;
    private static final double samplingThreshold = 0.04;

    public FullMembershipSet(final int rowCount) throws NegativeArraySizeException {
        if (rowCount >= 0)
            this.rowCount = rowCount;
        else
            throw (new NegativeArraySizeException("Can't initialize FullMembership with " +
                        "negative rowCount"));
    }

    @Override
    public int getMax() {
        return this.rowCount;
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return rowIndex < this.rowCount;
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public IRowIterator getIterator() {
        return new FullMembershipIterator(this.rowCount);
    }

    /**
     * The procedure
     * samples k times with replacement so it may return a set with less than k distinct items
     * @param k the number of samples taken with replacement
     * @param seed the seed for the randomness generator
     * @return IMembershipSet instantiated as a partial sparse
     */
    @Override
    public IMembershipSet sample(final int k, final long seed) {
        if (k >= this.rowCount)
            return new FullMembershipSet(this.rowCount);
        final Randomness randomGenerator = new Randomness(seed);
        return this.sampleUtil(randomGenerator, k);
    }

    private IMembershipSet sampleUtil(final Randomness randomGenerator, final int k) {
        int l = k;
        if (k > (int)(this.rowCount * 0.7)) // sample the items that are not returned
            l = this.rowCount - k;
        IMutableMembershipSet s = MembershipSetFactory.create(this.getMax(), k);
        for (int i=0; i < l; i++)
            s.add(randomGenerator.nextInt(this.rowCount));
        while (s.size() < l)
            s.add(randomGenerator.nextInt(this.rowCount));
        if (l == k)
            return s.seal();
        else
            return this.setMinus(s.seal());
    }

    /**
     * Returns an iterator that runs over the sampled data.
     * @param rate  Sampling rate.
     * @param seed  Random seed.
     * @return      An iterator over the sampled data.
     */
    @Override
    public ISampledRowIterator getIteratorOverSample(double rate, long seed, boolean enforceRate) {
        double effectiveRate;
        if (enforceRate)
            effectiveRate = rate;
        else
            effectiveRate = computeRate(rate);
        if (effectiveRate >= 1)
            return new NoSampleRowIterator(this.getIterator());
        else
            return new FullSampledRowIterator (rowCount, rate, seed);
    }

    /**
     * Returns the best rate to sample the data given the rate the user asked for
     * @return the actual rate to sample the data
     */
    private double computeRate(double rate) {
        if (rate  <= FullMembershipSet.samplingThreshold)
            return rate;
        else
            return 1;
    }

    private static class FullSampledRowIterator implements ISampledRowIterator {
        private int cursor = 0;
        private final int range;
        private final double rate;
        private Randomness prg;

        public FullSampledRowIterator(final int range, double rate, long seed) {
            this.rate = rate;
            this.range = range;
            this.prg = new Randomness(seed);
        }

        @Override
        public double rate() { return this.rate; }

        @Override
        public int getNextRow() {
            this.cursor += this.prg.nextGeometric(rate);
            if (this.cursor < this.range)
                return this.cursor;
            else return - 1;
        }
    }

    public static class FullMembershipIterator implements IRowIterator {
        private int cursor = 0;
        private final int range;

        public FullMembershipIterator(final int range) {
            this.range = range;
        }

        @Override
        public int getNextRow() {
            if (this.cursor < this.range) {
                this.cursor++;
                return this.cursor - 1;
            }
            else return - 1;
        }
    }
}

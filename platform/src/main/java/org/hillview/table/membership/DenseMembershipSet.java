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

import java.util.BitSet;

/**
 * A dense membership set.
 */
public class DenseMembershipSet implements IMembershipSet, IMutableMembershipSet {
    private final BitSet membershipMap;
    private final int max;
    private int size;
    private final static double samplingThreshold = 0.05;
    private final static double samplingSizeMinimum = 100; // if size is smaller than this no need to sample

    public DenseMembershipSet(int max, int expectedSize) {
        this.membershipMap = new BitSet(expectedSize);
        this.max = max;
        this.size = 0;
    }

    @Override
    public int getMax() {
        return this.max;
    }

    @Override
    public boolean isMember(int rowIndex) {
        return this.membershipMap.get(rowIndex);
    }

    private IMembershipSet denseSample(int k, long seed) {
        if (k >= this.size)
            return this;

        final Randomness psg = new Randomness(seed);
        int[] chosen = new int[k];
        int i, row = -1;

        // Reservoir sampling from this current set of bits
        IRowIterator ri = this.getIterator();
        for (i = 0; i < k; ++ i) {
            row = ri.getNextRow();
            assert row >= 0;
            chosen[i] = row;
        }
        row = ri.getNextRow();
        for (; row >= 0; ++ i) {
            int j = psg.nextInt(i+1);
            if (j < k)
                chosen[j] = row;
            row = ri.getNextRow();
        }

        IMutableMembershipSet mms = MembershipSetFactory.create(this.getMax(), k);
        for (i=0; i < k; i++)
            mms.add(chosen[i]);
        return mms.seal();
    }

    @Override
    public IMembershipSet sample(int k, long seed) {
        if (k > 0.7 * this.size)
            return this.denseSample(k, seed);
        final int numOfTries = 5;
        final Randomness psg = new Randomness(seed);
        IMutableMembershipSet mms = MembershipSetFactory.create(this.getMax(), k);
        int i = 0;
        while ((i < numOfTries * k) && (mms.size() < k)){
            int index = psg.nextInt(this.membershipMap.length());
            if (this.membershipMap.get(index))
                    mms.add(index);
            i++;
        }
        return mms.seal();
    }

    @Override
    public void add(int index) {
        if (this.membershipMap.get(index))
            return;
        this.membershipMap.set(index);
        this.size++;
    }

    @Override
    public IMembershipSet seal() {
        return this;
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public int size() { return this.size; }

    @Override
    public IRowIterator getIterator() {
        return new DenseMembershipIterator(this.membershipMap);
    }

    @Override
    public ISampledRowIterator getIteratorOverSample(double rate, long seed, boolean enforceRate) {
        double usedRate;
        if (enforceRate)
            usedRate = rate;
        else
            usedRate = computeRate(rate);
        if (usedRate >= 1)
            return new NoSampleRowIterator(this.getIterator());
        return new DenseSampledRowIterator (this.membershipMap, usedRate, seed);
    }

    private double computeRate(double rate) {
        if (this.size <  DenseMembershipSet.samplingSizeMinimum)
            return 1;
        if (rate <= DenseMembershipSet.samplingThreshold)
            return rate;
        else return 1;
    }

    private static class DenseSampledRowIterator implements ISampledRowIterator {
        private final BitSet bits;
        private final Randomness prg;
        private final double rate;
        int cursor = -1;

        DenseSampledRowIterator(BitSet bits, double rate, long seed) {
            this.bits = bits;
            this.prg = new Randomness(seed);
            this.rate = rate;
        }

        @Override
        public int getNextRow() {
            this.cursor += this.prg.nextGeometric(rate);
            while (this.cursor < this.bits.size()) {
                if (this.bits.get(cursor))
                    return this.cursor;
                this.cursor += this.prg.nextGeometric(rate);
            }
            return - 1;
        }

        @Override
        public double rate() { return this.rate; }
    }

    public static class DenseMembershipIterator implements IRowIterator {
        private final BitSet bits;
        private int current;

        DenseMembershipIterator(BitSet bits) {
            this.bits = bits;
            this.current = -1;
        }

        @Override
        public int getNextRow() {
            this.current = this.bits.nextSetBit(this.current + 1);
            return this.current;
        }
    }
}

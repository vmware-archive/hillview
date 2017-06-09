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
 *
 */

package org.hillview.table;

import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.utils.IntSet;
import org.hillview.utils.Randomness;

import java.util.function.Predicate;

/**
 * A IMembershipSet which contains all rows.
 */
public class FullMembership implements IMembershipSet {
    private final int rowCount;

    public FullMembership(final int rowCount) throws NegativeArraySizeException {
        if (rowCount >= 0)
            this.rowCount = rowCount;
        else
            throw (new NegativeArraySizeException("Can't initialize FullMembership with " +
                        "negative rowCount"));
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
     * Samples k items. Generator is seeded using its default method. Sampled items are
     * first placed in a Set. The procedure samples k times with replacement so it
     * may return a set with less than k distinct items.
     *
     * @param k the number of samples with replacement
     * @return IMembershipSet instantiated as a Partial Sparse
     */
    @Override
    public IMembershipSet sample(final int k) {
        if (k >= this.rowCount)
            return new FullMembership(this.rowCount);
        final Randomness randomGenerator = new Randomness();
        return this.sampleUtil(randomGenerator, k);
    }

    @Override
    public IMembershipSet filter(Predicate<Integer> predicate) {
        return new SparseMembership(this, predicate);
    }

    /**
     * Same as sample(k) but with the seed of the generator given as a parameter. The procedure
     * samples k times with replacement so it may return a set with less than k distinct items
     * @param k the number of samples taken with replacement
     * @param seed the seed for the randomness generator
     * @return IMembershipSet instantiated as a partial sparse
     */
    @Override
    public IMembershipSet sample(final int k, final long seed) {
        if (k >= this.rowCount)
            return new FullMembership(this.rowCount);
        final Randomness randomGenerator = new Randomness();
        randomGenerator.setSeed(seed);
        return this.sampleUtil(randomGenerator, k);
    }

    @Override
    public IMembershipSet union(final IMembershipSet otherSet) {
        if (otherSet instanceof FullMembership)
            return new FullMembership(Integer.max(this.rowCount, otherSet.getSize()));
        return otherSet.union(this);
    }

    @Override
    public IMembershipSet intersection(final IMembershipSet otherSet) {
        if (otherSet instanceof FullMembership)
            return new FullMembership(Integer.min(this.rowCount, otherSet.getSize()));
        return otherSet.intersection(this);
    }

    @Override
    public IMembershipSet setMinus(final IMembershipSet otherSet) {
        if (otherSet instanceof FullMembership) {
            final IntSet baseMap = new IntSet(Integer.max(0, this.getSize()-otherSet.getSize()));
            for (int i = otherSet.getSize(); i < this.rowCount; i++)
                baseMap.add(i);
            return new SparseMembership(baseMap);
        }
        final IntSet baseMap = new IntSet();
        for (int i = 0; i < this.getSize(); i++)
            if (!otherSet.isMember(i))
                baseMap.add(i);
        return new SparseMembership(baseMap);
    }

    private IMembershipSet sampleUtil(final Randomness randomGenerator, final int k) {
        int l = k;
        if (k > (int) (this.rowCount * 0.7)) // sample the items that are not returned
            l = this.rowCount - k;
        final IntSet s = new IntSet(l);
        for (int i=0; i < l; i++)
            s.add(randomGenerator.nextInt(this.rowCount));
        while (s.size() < l)
            s.add(randomGenerator.nextInt(this.rowCount));
        if (l == k)
            return new SparseMembership(s);
        else
            return this.setMinus(new SparseMembership(s));
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

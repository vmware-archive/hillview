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
import org.hillview.utils.IntSet;
import org.hillview.utils.Randomness;

/**
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 */
public class SparseMembershipSet implements IMembershipSet, IMutableMembershipSet {
    private final IntSet membershipMap;
    /* If the Intset is smaller than this then the class would ask the IntSet to sort its array for the iterator.
     * The sorting takes time but makes the iterator faster */
    public static final int thresholdSortedIterator = 50000000;
    private final int max;

    @Override
    public int getMax() { return this.max; }

    private SparseMembershipSet(IntSet map, int max) {
        this.membershipMap = map;
        this.max = max;
    }

    public SparseMembershipSet(int max, int estimated) {
        this(new IntSet(estimated), max);
    }

    public void add(int index) {
        this.membershipMap.add(index);
    }

    /**
     * Create a membership set which contains the integers (start, ..., start + size)
     * @param start The first integer in the set.
     * @param size The number of integers in the set.
     */
    public SparseMembershipSet(int start, int size, int max) {
        this.membershipMap = new IntSet(size);
        for (int i = 0; i < size; i++)
            this.membershipMap.add(start + i);
        this.max = max;
    }

    public IMembershipSet seal() {
        return this;
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return this.membershipMap.contains(rowIndex);
    }

    @Override
    public int getSize() {
        return this.membershipMap.size();
    }

    @Override
    public int size() { return this.getSize(); }

    /**
     * Returns the k items from a random location in the map determined by a seed provided.
     * Note that the k items are not completely independent but also depend on the placement
     * done by the hash function.
     * Remark: Slight difference from the contract in the interface, which calls for the sample to
     * be independent with replacement. In order to obtain that call sample(1,seed) k times with
     * different seeds.
     */
    @Override
    public IMembershipSet sample(final int k, final long seed) {
        return new SparseMembershipSet(this.membershipMap.sample(k, seed), this.getMax());
    }

    @Override
    public IRowIterator getIterator() { return new SparseIterator(this.membershipMap); }

    /**
     * Returns an iterator that runs over the sampled data.
     * @param rate  Sampling rate.
     * @param seed  Random seed.
     * @return      An iterator over the sampled data.
     */
    @Override
    public ISampledRowIterator getIteratorOverSample(double rate, long seed, boolean enforceRate) {
        if (rate >= 1)
            return new NoSampleRowIterator(this.getIterator());
        // Using a lower rate is always beneficial so enforceRate is always assumed to be true
        return new SparseMembershipSet.SparseSampledRowIterator(rate, seed, this.membershipMap);
    }

    private static class SparseSampledRowIterator implements ISampledRowIterator {
        final int sampleSize;
        final Randomness psg;
        int currentSize = 0;
        int currentCursor;
        final IntSet mMap;
        final double rate;

        private SparseSampledRowIterator(final double rate, final long seed, IntSet mmap) {
            this.mMap = mmap;
            psg = new Randomness(seed);
            double bias = psg.nextDouble();
            if (bias < rate)
                this.sampleSize = (int) Math.floor(rate * mMap.size());
            else this.sampleSize = (int) Math.ceil(rate * mMap.size());
            currentCursor = psg.nextInt(mMap.arraySize());
            this.rate = rate;
        }

        @Override
        public double rate() { return this.rate; }

        @Override
        public int getNextRow() {
            if (this.currentSize >= this.sampleSize)
                return -1;
            if ((this.currentSize == this.sampleSize - 1) && mMap.contains(0))
                if (this.psg.nextDouble() < 1 / this.sampleSize) {
                    this.currentSize++;
                    return 0;
                }
            int index = mMap.getNext(currentCursor);
            this.currentSize++;
            this.currentCursor = index + 1;
            return mMap.probe(index);
        }
    }

    private static class SparseIterator implements IRowIterator {
        final private IntSet.IntSetIterator mySetIterator;

        private SparseIterator(final IntSet mySet) { this.mySetIterator = mySet.getIterator(); }

        @Override
        public int getNextRow() {
            return this.mySetIterator.getNext();
        }
    }
}

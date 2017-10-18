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
import org.hillview.utils.IntSet;

/**
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 */
public class SparseMembershipSet implements IMembershipSet, IMutableMembershipSet {
    private final IntSet membershipMap;
    private final int max;

    @Override
    public int getMax() { return this.max; }

    /**
     * Essentially wraps a Set interface by IMembershipSet
     * @param baseSet of type Set
     */
    public SparseMembershipSet(final IntSet baseSet, int max) {
        this.membershipMap = baseSet;
        this.max = max;
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
        return new SparseMembershipSet(this.membershipMap.sample(k, seed, true), this.getMax());
    }

    @Override
    public IRowIterator getIterator() {
        return new SparseIterator(this.membershipMap);
    }

    @Override
    public IMembershipSet union(final IMembershipSet otherSet) {
        final IntSet unionSet = this.membershipMap.copy();
        final IRowIterator iter = otherSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >=0) {
            unionSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembershipSet(unionSet, this.getMax());
    }

    @Override
    public IMembershipSet intersection(final IMembershipSet otherSet) {
        final IntSet intersectSet = new IntSet();
        final IRowIterator iter = otherSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (this.isMember(curr))
                intersectSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembershipSet(intersectSet, this.getMax());
    }


    private static class SparseIterator implements IRowIterator {
        final private IntSet.IntSetIterator mySetIterator;

        private SparseIterator(final IntSet mySet) {
            this.mySetIterator = mySet.getIterator();
        }

        @Override
        public int getNextRow() {
            return this.mySetIterator.getNext();
        }
    }
}

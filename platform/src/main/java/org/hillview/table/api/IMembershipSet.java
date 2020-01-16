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

package org.hillview.table.api;

import org.hillview.table.membership.MembershipSetFactory;
import org.hillview.utils.Randomness;
import org.hillview.utils.Utilities;

import java.util.function.IntPredicate;

/**
 * A IMembershipSet is a representation of a set of integers between 0 and max.
 * These integers represent row indexes in a table.  If an integer
 * is in an IMembershipSet, then it is present in the table.
 */
public interface IMembershipSet extends IRowOrder {
    /**
     * @return The size of the original set that this membership set is a part of.
     */
    int getMax();

    /**
     * @param rowIndex A non-negative row index.
     * @return True if the given rowIndex is a member of the set.
     */
    boolean isMember(int rowIndex);

    /**
     * Return a membership containing only the rows in the current one where
     * the predicate evaluates to true.
     * @param predicate  Predicate evaluated for each row.
     */
    default IMembershipSet filter(IntPredicate predicate) {
        int estimatedSize = MembershipSetFactory.estimateSize(this, predicate);
        IMutableMembershipSet ms = MembershipSetFactory.create(this.getMax(), estimatedSize);

        IRowIterator baseIterator = this.getIterator();
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            if (predicate.test(tmp))
                ms.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        return ms.seal();
    }

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * without replacement. Returns the full set if its size is smaller than k. The pseudo-random
     * generator is seeded with parameter seed.
     */
    IMembershipSet sample(int k, long seed);

    /**
     * Returns an iterator that runs over the sampled data.
     * @param rate  Sampling rate.
     * @param seed  Random seed.
     * @param enforceRate   If true the sampling rate given must be used; otherwise the result
     *                      may use a different sampling rate if it is more efficient to do so.
     *                      The resulting iterator has information about the sampling rate used.
     * @return      An iterator over the sampled data.
     */
    ISampledRowIterator getIteratorOverSample(double rate, long seed, boolean enforceRate);

    /**
     * @return a sample of size (rate * rowCount). randomizes between the floor and ceiling of this expression.
     */
    default IMembershipSet sample(double rate, long seed) {
        if (rate <= 0)
            throw new RuntimeException("Sampling rate of 0");
        return this.sample(this.getSampleSize(rate, seed), seed);
    }

    default IMembershipSet setMinus(IMembershipSet other) {
        IMutableMembershipSet mms = MembershipSetFactory.create(
                this.getMax(), this.getSize() - other.getSize());
        final IRowIterator iter = this.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (!other.isMember(curr))
                mms.add(curr);
            curr = iter.getNextRow();
        }
        return mms.seal();
    }

    default IMembershipSet union(final IMembershipSet other) {
        IMutableMembershipSet mms = MembershipSetFactory.create(
                this.getMax(), this.getSize() + other.getSize());
        IRowIterator iter = this.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            mms.add(curr);
            curr = iter.getNextRow();
        }

        iter = other.getIterator();
        curr = iter.getNextRow();
        while (curr >= 0) {
            mms.add(curr);
            curr = iter.getNextRow();
        }
        return mms.seal();
    }

    default IMembershipSet intersection(final IMembershipSet other) {
        IMutableMembershipSet mms = MembershipSetFactory.create(
                this.getMax(), Math.min(this.getSize(), other.getSize()));
        final IRowIterator iter = this.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (other.isMember(curr))
                mms.add(curr);
            curr = iter.getNextRow();
        }
        return mms.seal();
    }

    default int getSampleSize(double rate, long seed) {
        if (rate >= 1)
            return this.getSize();
        Randomness r = new Randomness(seed);
        final int sampleSize;
        final double appSampleSize = rate * this.getSize();
        if (r.nextDouble() < (appSampleSize - Math.floor(appSampleSize)))
            sampleSize = Utilities.toInt(Math.floor(appSampleSize));
        else sampleSize = Utilities.toInt(Math.ceil(appSampleSize));
        return sampleSize;
    }

    /**
     * Return all the rows in this membership set in an array.
     * The membership set should be small.
     */
    default int[] getRows() {
        final int size = this.getSize();
        final int[] result = new int[size];
        IRowIterator ri = this.getIterator();
        int row = ri.getNextRow();
        int index = 0;
        while (row >= 0) {
            result[index++] = row;
            row = ri.getNextRow();
        }
        assert index == size;
        return result;
    }

    /**
     * Policy which indicates when to use a sparse column when
     * storing only `size` elements.
     * @param size: Expected number of elements in a target column.
     * @return      True when the target column should be sparse.
     */
    default boolean useSparseColumn(int size) {
        return this.getMax() > 3 * size;
    }

    default boolean useSparseColumn() {
        return this.useSparseColumn(this.getSize());
    }
}

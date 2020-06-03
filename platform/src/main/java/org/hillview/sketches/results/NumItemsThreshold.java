/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.sketches.results;

import net.openhft.hashing.LongHashFunction;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.dataset.api.ISketchResult;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.utils.Utilities;

import java.util.BitSet;

/**
 * A class that estimates whether the number of distinct items in a column is smaller than a threshold. Implemented by hashing
 * items into a bit array. Once enough bits are set the threshold had been crossed and the iterator may stop
 */
public class NumItemsThreshold implements IJsonSketchResult {
    static final long serialVersionUID = 1;
    
    private final int logThreshold;
    @SuppressWarnings("FieldCanBeLocal")
    private final int maxLogThreshold = 15;
    private final BitSet bits;
    private final long seed;
    /**
     * The threshold in terms of number of set bits
     */
    private final int bitThreshold;
    /**
     * log the size of the bitSet
     */
    private final int logSize;

    public NumItemsThreshold(int logThreshold, long seed) {
        if ((logThreshold < 1) || (logThreshold > maxLogThreshold))
            throw new IllegalArgumentException("NumItemsThreshold called with illegal size of " + logThreshold);
        this.seed = seed;
        this.logThreshold = logThreshold;
        int threshold = 1 << logThreshold;
        if (threshold >= 1024) {
            logSize = logThreshold;
            bits = new BitSet(threshold);
            // When the number of bits is equal to the threshold we expect 1-1/e = 0.6322 of the bits to be set. On top
            // of that we add sqrt of the threshold for a high probability bound.
            bitThreshold = Utilities.toInt(Math.round(0.6322 * threshold + Math.sqrt(threshold)));
        } else {  // if the threshold is small we want the bitSet still to be large enough to provide sufficient accuracy
            logSize = 10;
            bits = new BitSet(1024);
            double expo = -threshold / 1024.0;
            bitThreshold = Utilities.toInt(Math.round(((1 - Math.pow(2.7182, expo)) * 1024) + Math.sqrt(threshold)));
        }
    }

    public void createBits(IColumn column, IMembershipSet memSet) {
        final IRowIterator myIter = memSet.getIterator();
        LongHashFunction hash = LongHashFunction.xx(this.seed);
        int currRow = myIter.getNextRow();
        int cardinality = 0;
        while ((currRow >= 0) && (cardinality < bitThreshold)) { // if threshold reached stop iterating
            if (!column.isMissing(currRow)) {
                long itemHash = column.hashCode64(currRow, hash);
                int index =  (int) itemHash >>> (Long.SIZE - this.logSize);
                if (!bits.get(index))
                    cardinality++;
                this.bits.set(index);
            }
            currRow = myIter.getNextRow();
        }
    }

    public NumItemsThreshold union(NumItemsThreshold otherNIT) {
        NumItemsThreshold result = new NumItemsThreshold(this.logThreshold, this.seed);
        result.bits.or(this.bits);
        result.bits.or(otherNIT.bits);
        return result;
    }

    public boolean exceedThreshold() {
        return (bits.cardinality() >= bitThreshold);
    }
}

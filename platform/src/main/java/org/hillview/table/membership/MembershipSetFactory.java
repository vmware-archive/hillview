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

import java.util.function.IntPredicate;

/**
 * This class knows how to create a membership set.
 */
public class MembershipSetFactory {
    private static final int sizeEstimationSampleSize = 40;

    /**
     * Creates a mutable membership set.
     * @param maxSize        Maximum size.
     * @param estimatedSize  Estimated number of elements inside.
     */
    public static IMutableMembershipSet create(int maxSize, int estimatedSize) {
        if (estimatedSize >= maxSize / 30)
            return new DenseMembershipSet(maxSize, estimatedSize);
        else
            return new SparseMembershipSet(maxSize, estimatedSize);
    }

    /**
     * Estimates the size of a filter applied to an IMembershipSet
     * @return an approximation of the size based on sampling. May return 0.
     * There are no strict guarantees on the quality of the approximation,
     * but is good enough for initialization of a hash table sizes.
     */
    public static int estimateSize(final IMembershipSet baseMap,
                                   final IntPredicate filter) {
        final IMembershipSet sampleSet = baseMap.sample(sizeEstimationSampleSize, 0);
        if (sampleSet.getSize() == 0)
            return 0;
        int eSize = 0;
        final IRowIterator iter = sampleSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (filter.test(curr))
                eSize++;
            curr = iter.getNextRow();
        }
        return (baseMap.getSize() * eSize) / sampleSet.getSize();
    }

}

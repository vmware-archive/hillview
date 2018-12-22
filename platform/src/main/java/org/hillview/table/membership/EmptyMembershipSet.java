/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ISampledRowIterator;

/**
 * A membership set that has no rows whatsoever.
 */
public class EmptyMembershipSet implements IMembershipSet {
    public final int max;

    public EmptyMembershipSet(int max)  {
        this.max = max;
    }

    @Override
    public int getMax() {
        return this.max;
    }

    @Override
    public boolean isMember(int rowIndex) {
        return false;
    }

    @Override
    public IMembershipSet sample(int k, long seed) {
        return this;
    }

    @Override
    public ISampledRowIterator getIteratorOverSample(double rate, long seed, boolean enforceRate) {
        return () -> -1;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public IRowIterator getIterator() {
        return () -> -1;
    }
}

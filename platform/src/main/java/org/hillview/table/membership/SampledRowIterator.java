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

import org.hillview.table.api.IRowIterator;
import org.hillview.utils.Randomness;

public class SampledRowIterator implements IRowIterator {
    final IRowIterator base;
    final Randomness randomness;
    final double rate;

    public SampledRowIterator(IRowIterator base, double rate, long seed) {
        this.base = base;
        this.rate = rate;
        this.randomness = new Randomness(seed);
    }

    @Override
    public int getNextRow() {
        while (true) {
            int next = this.base.getNextRow();
            if (next < 0)
                return next;
            if (randomness.nextDouble() <= this.rate)
                return next;
        }
    }
}

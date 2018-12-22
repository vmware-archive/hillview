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

import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.IRowOrder;

/**
 * Represents a set of consecutive rows starting at `start`.
 */
public class BlockRowOrder implements IRowOrder {
    private final int start;
    private final int count;

    public BlockRowOrder(int start, int count) {
        assert(start >= 0);
        assert(count >= 0);
        this.start = start;
        this.count = count;
    }

    @Override
    public int getSize() {
        return this.count;
    }

    class Iterator implements IRowIterator {
        private int current;

        Iterator() {
            current = 0;
        }

        @Override
        public int getNextRow() {
            if (this.current < BlockRowOrder.this.count) {
                this.current++;
                return BlockRowOrder.this.start + this.current - 1;
            }
            return -1;
        }
    }

    @Override
    public IRowIterator getIterator() {
        return new Iterator();
    }
}

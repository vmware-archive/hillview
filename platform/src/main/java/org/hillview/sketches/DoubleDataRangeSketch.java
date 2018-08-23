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

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * A sketch that computes the range of data in a column where values can be
 * converted to doubles.
 */
public class DoubleDataRangeSketch implements ISketch<ITable, DataRange> {
    private final String col;

    public DoubleDataRangeSketch(String col) {
        this.col = col;
    }

    @Override
    public DataRange create(final ITable data) {
        IColumn column = data.getLoadedColumn(this.col);
        DataRange result = new DataRange();
        final IRowIterator myIter = data.getMembershipSet().getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!column.isMissing(currRow)) {
                double val = column.asDouble(currRow);
                result.add(val);
            } else {
                result.addMissing();
            }
            currRow = myIter.getNextRow();
        }
        return result;
    }

    @Override
    public DataRange zero() { return new DataRange(); }

    @Override
    public DataRange add(@Nullable final DataRange left, @Nullable final DataRange right) {
        assert left != null;
        assert right != null;
        return left.add(right);
    }
}

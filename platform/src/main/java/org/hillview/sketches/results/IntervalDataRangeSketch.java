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

package org.hillview.sketches.results;

import org.hillview.sketches.DoubleDataRangeSketch;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A sketch that computes the range of data in a column where values are intervals.
 */
public class IntervalDataRangeSketch extends DoubleDataRangeSketch {
    static final long serialVersionUID = 1;

    public IntervalDataRangeSketch(String col) {
        super(col);
    }

    @Override
    public DataRange create(@Nullable final ITable data) {
        IColumn column = Converters.checkNull(data).getLoadedColumn(this.col);
        assert(column.getKind() == ContentsKind.Interval);
        DataRange result = new DataRange();
        final IRowIterator myIter = data.getMembershipSet().getIterator();
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!column.isMissing(currRow)) {
                double val = column.getEndpoint(currRow, true);
                result.add(val);
                val = column.getEndpoint(currRow, false);
                result.add(val);
            } else {
                result.addMissing();
            }
            currRow = myIter.getNextRow();
        }
        return result;
    }
}

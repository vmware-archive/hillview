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

import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;

import javax.annotation.Nullable;

/**
 * A class that scans a column and collects basic statistics: maximum, minimum,
 * number of non-empty rows and the moments of asDouble values.
 */
public class StringColumnStatistics extends ColumnStatistics {
    @Nullable
    private String min;
    @Nullable
    private String max;

    public StringColumnStatistics() {
        this.min = null;
        this.max = null;
    }

    void createStats(final ColumnAndConverter column,
                     final IMembershipSet membershipSet) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();

        while (currRow >= 0) {
            if (column.column.isMissing(currRow)) {
                this.missingCount++;
                currRow = myIter.getNextRow();
                continue;
            }

            String strVal = column.getString(currRow);
            if (this.presentCount == 0) {
                this.min = strVal;
                this.max = strVal;
            } else {
                assert this.min != null;
                assert strVal != null;
                if (this.min.compareTo(strVal) < 0)
                    this.min = strVal;
                assert this.max != null;
                if (this.max.compareTo(strVal) < 0)
                    this.max = strVal;
            }

            this.presentCount++;
            currRow = myIter.getNextRow();
        }
    }

    public ColumnStatistics union(final ColumnStatistics other) {
        StringColumnStatistics otherStat = (StringColumnStatistics)other;
        StringColumnStatistics result = new StringColumnStatistics();
        result.presentCount = this.presentCount + otherStat.presentCount;
        result.missingCount = this.missingCount + otherStat.missingCount;

        if (this.presentCount == 0) {
            result.min = otherStat.min;
            result.max = otherStat.max;
        } else if (otherStat.presentCount == 0) {
            result.min = this.min;
            result.max = this.max;
        } else {
            if (this.min == null)
                result.min = otherStat.min;
            else if (otherStat.min == null)
                result.min = this.min;
            else if (this.min.compareTo(otherStat.min) < 0) {
                result.min = this.min;
            } else {
                result.min = otherStat.min;
            }

            if (this.max == null)
                result.max = otherStat.max;
            else if (otherStat.max == null)
                result.max = this.max;
            else if (this.max.compareTo(otherStat.max) > 0) {
                result.max = this.max;
            } else {
                result.max = otherStat.max;
            }
        }
        return result;
    }
}

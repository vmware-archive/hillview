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

package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A class that scans a column and collects basic statistics: maximum, minimum,
 * number of non-empty rows and the moments of asDouble values.
 */
public class BasicColStats extends DataRange implements IJson {
    private final int momentCount;
    private final boolean computeStringMax;
    @Nullable
    public String minString;
    @Nullable
    public String maxString;
    public final double[] moments;

    public BasicColStats(int momentCount, boolean computeStringMax) {
        if (momentCount < 0)
            throw new IllegalArgumentException("number of moments cannot be negative");
        this.momentCount = momentCount;
        this.moments = new double[this.momentCount];
        this.min = 0;  // we cannot use infinity, since that cannot be serialized as JSON
        this.max = 0;
        this.presentCount = 0;
        this.minString = null;
        this.maxString = null;
        this.computeStringMax = computeStringMax;
    }

    public double getMin() { return this.min; }
    public double getMax() { return this.max; }
    /**
     *
     * @param i Moment number; note that moments are numbered from 1, not 0.
     * @return the i'th moment: the normalized sum of x^i
     */
    public double getMoment(int i) {
        return this.moments[i - 1];
    }

    /**
     * @return the number of non-missing rows in a column
     */
    public long getPresentCount() { return this.presentCount; }
    public long getRowCount() { return this.presentCount + this.missingCount; }

    void scan(final IColumn column,
              final IMembershipSet membershipSet) {
        final IRowIterator myIter = membershipSet.getIterator();
        int currRow = myIter.getNextRow();

        boolean extractString = false;
        boolean numeric = false;
        switch (column.getKind()) {
            case String:
            case Json:
                extractString = this.computeStringMax;
                break;
            default:
                numeric = true;
                break;
        }
        while (currRow >= 0) {
            if (column.isMissing(currRow)) {
                this.missingCount++;
                currRow = myIter.getNextRow();
                continue;
            }

            String strVal = null;
            double val = 0;
            if (numeric)
                val = column.asDouble(currRow);
            else if (extractString)
                strVal = column.getString(currRow);
            if (this.presentCount == 0) {
                if (numeric) {
                    this.min = val;
                    this.max = val;
                }
                if (extractString) {
                    this.minString = strVal;
                    this.maxString = strVal;
                }
            } else {
                if (numeric) {
                    if (val < this.min) {
                        this.min = val;
                    } else if (val > this.max) {
                        this.max = val;
                    }
                }
                if (extractString) {
                    assert this.minString != null;
                    assert strVal != null;
                    if (this.minString.compareTo(strVal) > 0)
                        this.minString = strVal;
                    assert this.maxString != null;
                    if (this.maxString.compareTo(strVal) < 0)
                        this.maxString = strVal;
                }
            }

            if (numeric) {
                if (this.momentCount > 0) {
                    double tmpMoment = val;
                    double alpha = (double) this.presentCount / (double) (this.presentCount + 1);
                    double beta = 1.0 - alpha;
                    this.moments[0] = (alpha * this.moments[0]) + (beta * val);
                    for (int i = 1; i < this.momentCount; i++) {
                        tmpMoment = tmpMoment * val;
                        this.moments[i] = (alpha * this.moments[i]) + (beta * tmpMoment);
                    }
                }
            }
            this.presentCount++;
            currRow = myIter.getNextRow();
        }
    }

    /**
     * @param otherStat The other BasicColStats structure to be merged with this.
     * @return The merge of the two.
     */
    public BasicColStats union(final BasicColStats otherStat) {
        BasicColStats result = new BasicColStats(this.momentCount, this.computeStringMax);
        result.presentCount = this.presentCount + otherStat.presentCount;
        result.missingCount = this.missingCount + otherStat.missingCount;

        if (this.presentCount == 0) {
            result.min = otherStat.min;
            result.max = otherStat.max;
            result.minString = otherStat.minString;
            result.maxString = otherStat.maxString;
        } else if (otherStat.presentCount == 0) {
            result.min = this.min;
            result.max = this.max;
            result.minString = this.minString;
            result.maxString = this.maxString;
        } else {
            result.min = Math.min(this.min, otherStat.min);
            result.max = Math.max(this.max, otherStat.max);
            result.minString = Converters.min(this.minString, otherStat.minString);
            result.maxString = Converters.max(this.maxString, otherStat.maxString);
        }
        if (result.presentCount > 0) {
            double alpha = (double) this.presentCount / ((double) result.presentCount);
            double beta = 1.0 - alpha;
            for (int i = 0; i < this.momentCount; i++)
                result.moments[i] = (alpha * this.moments[i]) + (beta * otherStat.moments[i]);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("present=").append(this.presentCount);
        if (this.computeStringMax) {
            result.append(",min=").append(this.minString);
            result.append(",max=").append(this.maxString);
        } else if (this.presentCount > 0) {
            result.append(",min=").append(this.min);
            result.append(",max=").append(this.max);
            for (int i = 0; i < this.momentCount; i++)
                result.append(",moment[").append(i).append("]=").append(this.moments[i]);
        }
        return result.toString();
    }
}

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
 *
 */

package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IRowOrder;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

/**
 * QuantileList is the data structure for storing Quantile information. It consists  of
 * - dataSize: the size of the set that the quantiles are calculated over.
 * - quantile: A table containing a sorted list of elements.
 * - approxRank: an array containing approximate ranks for each element in the Table.
 * Each entry has two fields: wins and losses. Wins represents the number of elements that are
 * definitely less than  this element, Losses represents the number that are greater. For the
 * remaining elements, we do not what know the result of the comparison will be. Hence for any element:
 *      Wins + Losses < dataSize.
 */
public class QuantileList implements Serializable {
    static class WinsAndLosses {
        public final int wins;
        public final int losses;

        public WinsAndLosses(final int wins, final int losses) {
            this.wins = wins;
            this.losses = losses;
        }

        public String toString() {
            return String.valueOf(this.wins) + ", " + String.valueOf(this.losses);
        }
    }

    public final SmallTable quantile;
    private final WinsAndLosses[] winsAndLosses;
    private final int dataSize;

    /**
     * An empty quantile list for a table with the specified schema.
     */
    public QuantileList(Schema schema) {
        this.winsAndLosses = new WinsAndLosses[0];
        this.dataSize = 0;
        this.quantile = new SmallTable(schema);
    }

    public QuantileList(final SmallTable quantile, final WinsAndLosses[] winsAndLosses, final int dataSize) {
        this.winsAndLosses = winsAndLosses;
        if (quantile.getNumOfRows() != winsAndLosses.length)
            throw new InvalidParameterException("Two arguments have different lengths");
        this.quantile = quantile;
        this.dataSize = dataSize;
    }

    /**
     * @return The number of elements in the list of quantiles
     */
    public int getQuantileSize() { return this.quantile.getNumOfRows(); }

    /**
     * @return The number of input rows over which these quantiles have been computed.
     */
    public int getDataSize() { return this.dataSize; }

    public IColumn getColumn(final String colName) {
        return this.quantile.getColumn(colName);
    }

    public Schema getSchema() {
        return this.quantile.getSchema();
    }

    public RowSnapshot getRow(final int rowIndex) {
        return new RowSnapshot(this.quantile, rowIndex);
    }

    /**
     * @param rowIndex The index of an element in the table quantile (call it x).
     * @return The number of elements in the dataset that are known to be less than x.
     */
    public int getWins(final int rowIndex) { return this.winsAndLosses[rowIndex].wins; }

    /**
     * @param rowIndex The index of an element in the table quantile (call it x).
     * @return The number of elements in the dataset that are known to be greater than x.
     */
    public int getLosses(final int rowIndex) { return this.winsAndLosses[rowIndex].losses; }

    /**
     * @param rowIndex The index of an element in the table quantile (call it x).
     * @return The Win and Loss record for that element.
     */
    private WinsAndLosses getWinsAndLosses(final int rowIndex) { return this.winsAndLosses[rowIndex]; }

    /**
     * Given an element in the QuantileList (specified as an index in the table), return an estimate
     * for the rank of that element in sorted (ascending) order.
     * @param rowIndex The index of an element x in the table quantile.
     * @return A guess for the rank of the element x. We know it lies in
     * the interval (wins(x), dataSize - losses(x)), so we return the average of the two.
     */
    private double getApproxRank(final int rowIndex) {
        return ((((double) this.getWins(rowIndex) + this.getDataSize()) -
                this.getLosses(rowIndex)) / 2);
    }

    /**
     * Helper method that given a RowOrder as input, compresses the QuantileList down to contain
     * only the sequence of rows specified by the input RowOrder.
     * @param rowOrder The subset of Rows (and their ordering)
     * @return A new QuantileList
     */
    private QuantileList compress(IRowOrder rowOrder) {
        WinsAndLosses[] newRank = new WinsAndLosses[rowOrder.getSize()];
        final IRowIterator rowIt = rowOrder.getIterator();
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i < 0) { break; }
            newRank[row] = this.getWinsAndLosses(i);
            row++;
        }
        return new QuantileList(this.quantile.compress(rowOrder), newRank, this.dataSize);
    }

    /** Given a desired size parameter (newSize), compress down to nearly the desired size.
     * In detail, we define the average gap to be the dataSize/newSize.
     * We greedily discard an entry if the gap between the previous and next
     * entry in the quantile is less than the average gap. This will result in a list whose
     * size is between newSize and 2*newSize.
     * @param newSize The desired size of the compressed table.
     * @return A QuantileList whose size lies in (newSize, 2*newSize), unless the size of the List
     * is already smaller than newSize, in which case we return the existing List.
     */
    public QuantileList compressApprox(int newSize) {
        int oldSize = this.getQuantileSize();
        if (oldSize <= newSize) { return this; }
        double avgGap = ((double) this.getDataSize()) / (newSize+1);
        List<Integer> newSubset = new ArrayList<>();
        newSubset.add(0);
        double open = this.getApproxRank(0);
        double close;
        for (int i = 1; i <= (oldSize - 2); i++) {
            close = this.getApproxRank(i+1);
            if ((close - open) > avgGap) {
                newSubset.add(i);
                open = this.getApproxRank(i);
            }
        }
        newSubset.add(oldSize - 1);
        IRowOrder rowOrder = new ArrayRowOrder(newSubset);
        return this.compress(rowOrder);
    }

    /** Given a desired size parameter (newSize), compress down to exactly that size.
     *  Let stepSize = dataSize/(newSize +1). The "target" rank for element i is (i +1)* StepSize.
     *  We pick the element of the QuantileList whose approxRank is the closest to this.
     * @param newSize The desired size of the compressed table.
     * @return A QuantileList of size newSize, computed as described above.
     */
    public QuantileList compressExact(int newSize) {
        int oldSize = this.getQuantileSize();
        if (oldSize <= newSize) { return this; }
        List<Integer> newSubset = new ArrayList<>();
        double stepSize = ((double) this.getDataSize()) / (newSize + 1);
        int j = 0;
        for (int i = 0; i < newSize; i++) {
            double targetRank = (i + 1) * stepSize;
            while (true) {
                double ar = this.getApproxRank(j);
                if ((ar <= targetRank) && (j <= (oldSize - 2)))
                    j++;
                else
                    break;
            }
            if (j == 0)
                newSubset.add(0);
            else {
            /* Check whether j or j-1 is closer to the targetRank */
                if ((this.getApproxRank(j) + this.getApproxRank(j - 1)) <= (2 * targetRank))
                    newSubset.add(j);
                else
                    newSubset.add(j - 1);
            }
        }
        IRowOrder rowOrder = new ArrayRowOrder(newSubset);
        return this.compress(rowOrder);
    }

    public String toString() {
        final StringBuilder builder = new StringBuilder();
        final IRowIterator rowIt = this.quantile.getRowIterator();
        int nextRow = rowIt.getNextRow();
        while (nextRow >= 0) {
            for (final String colName: this.quantile.getSchema().getColumnNames()) {
                builder.append(this.getColumn(colName).asString(nextRow));
                builder.append(", ");
            }
            builder.append("Approx Rank: ").append(this.getApproxRank(nextRow));
            builder.append(System.lineSeparator());
            nextRow = rowIt.getNextRow();
        }
        return builder.toString();
    }
}
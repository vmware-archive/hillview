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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.IStringFilter;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.StringFilterDescription;
import org.hillview.table.filters.StringFilterFactory;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import javax.annotation.Nullable;

public class FindSketch implements ISketch<ITable, FindSketch.Result> {
    public static final class Result implements IJson {
        /**
         * Number of occurrences of the search string (strictly) above the first row.
         */
        public final long before;
        /**
         * Number of occurrences of the first row.
         */
        public final long at;
        /**
         * Number of occurrences of the search string (strictly) below the first row.
         */
        public final long after;
        /**
         * First row that matches the string after the top row.
         */
        @Nullable
        public final RowSnapshot firstMatchingRow;

        Result() {
            this.before = 0;
            this.at = 0;
            this.after = 0;
            this.firstMatchingRow = null;
        }

        Result(long before, long at, long after,  @Nullable RowSnapshot firstMatchingRow) {
            this.before = before;
            this.at = at;
            this.after= after;
            this.firstMatchingRow = firstMatchingRow;
        }

        Result add(Result other, RecordOrder order) {
            @Nullable RowSnapshot fr;
            long before, at, after;
            if (this.firstMatchingRow == null) {
                fr = other.firstMatchingRow;
                before = this.before + other.before;
                at = other.at;
                after = other.after;
            } else if (other.firstMatchingRow == null) {
                fr = this.firstMatchingRow;
                before = this.before + other.before;
                at = this.at;
                after = this.after;
            } else {
                int compare = this.firstMatchingRow.compareTo(other.firstMatchingRow, order);
                before  = this.before + other.before;
                if (compare < 0) {
                    fr = this.firstMatchingRow;
                    at = this.at;
                    after = this.after + other.at + other.after;
                }
                else if (compare == 0 ) {
                    fr = this.firstMatchingRow;
                    at = this.at + other.at;
                    after = this.after + other.after;
                }
                else {
                    fr = other.firstMatchingRow;
                    at = other.at;
                    after = this.at + this.after + other.after;
                }
            }
            return new Result(before, at, after, fr);
        }

        @Override
        public JsonElement toJsonTree() {
            JsonObject object = new JsonObject();
            object.addProperty("before", this.before);
            object.addProperty("at", this.at);
            object.addProperty("after", this.after);
            if (this.firstMatchingRow == null)
                object.addProperty("firstMatchingRow", (String)null);
            else
                object.add("firstMatchingRow", this.firstMatchingRow.toJsonTree());
            return object;
        }
    }

    /**
     * Description of the string to search.
     */
    private final StringFilterDescription stringFilterDescription;
    /**
     * Only return rows greater than or equal to than this row.
     * If this is null there are no constraints.
     */
    @Nullable
    private final RowSnapshot topRow;
    /**
     * Order used for sorting data.
     */
    private final RecordOrder recordOrder;
    /**
     * If true, only return results strictly greater than the top row. Is used in implementing the
     * findNext functionality.
     */
    private final boolean excludeTopRow;
    /**
     * If true, we are finding the next string in sorted order (as specified by recordOrder). If
     * false, we are looking for the previous string (or equivalently the next string in reverse
     * sorted order.
     */
    private final boolean next;

    public FindSketch(final StringFilterDescription stringFilterDescription,
                      final @Nullable RowSnapshot topRow, final RecordOrder recordOrder,
                      final boolean excludeTopRow, final boolean next) {
        this.stringFilterDescription = stringFilterDescription;
        if ((!next) && (topRow == null))
                throw new RuntimeException("Top Row cannot be null");
        this.topRow = topRow;
        this.recordOrder = next ? recordOrder: recordOrder.reverse();
        this.excludeTopRow = !next || excludeTopRow;
        this.next = next;
    }

    public FindSketch(final StringFilterDescription stringFilterDescription,
                      final @Nullable RowSnapshot topRow,
                      final RecordOrder recordOrder, final boolean excludeTopRow) {
        this(stringFilterDescription, topRow, recordOrder, excludeTopRow, true);
    }

    public FindSketch(final StringFilterDescription stringFilterDescription,
                      final @Nullable RowSnapshot topRow,
                      final RecordOrder recordOrder) {
        this(stringFilterDescription, topRow, recordOrder, false, true);
    }


    @Override
    public Result create(ITable data) {
        long before = 0;
        long at = 0;
        long after = 0;
        IRowIterator rowIt = data.getRowIterator();
        Schema toCheck = this.recordOrder.toSchema();
        IStringFilter stringFilter = StringFilterFactory.getFilter(this.stringFilterDescription);
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data, toCheck);
        VirtualRowSnapshot smallestMatch = new VirtualRowSnapshot(data, toCheck);
        int compareTop, compareSmallest;
        for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
            vw.setRow(i);
            if (!vw.matches(stringFilter))
                continue;
            boolean match_before;
            if (this.topRow == null)
                match_before = false;
            else {
                compareTop = this.topRow.compareTo(vw, this.recordOrder);
                match_before = (compareTop > 0) || ((compareTop == 0) && this.excludeTopRow);
            }
            if (match_before)
                before += 1;
            else {
                if (at == 0) {
                    smallestMatch.setRow(i);
                    at = 1;
                } else {
                    compareSmallest = smallestMatch.compareTo(vw, this.recordOrder);
                    if (compareSmallest > 0) {
                        smallestMatch.setRow(i);
                        after += at;
                        at = 1;
                    } else if (compareSmallest == 0)
                        at += 1;
                    else
                        after += 1;
                }
            }
        }
        RowSnapshot firstRow;
        if (at == 0) {
            firstRow = null;
        } else {
            firstRow = smallestMatch.materialize();
        }
        if (!next) {
            long tmp = before;
            before = after;
            after = tmp;
        }
        return new Result(before, at, after, firstRow);
    }

    @Nullable
    @Override
    public Result zero() {
        return new Result();
    }

    @Nullable
    @Override
    public Result add(@Nullable Result left, @Nullable Result right) {
        assert left != null;
        assert right != null;
        return left.add(right, this.recordOrder);
    }
}

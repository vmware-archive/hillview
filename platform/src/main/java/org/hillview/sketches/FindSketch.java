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
import org.hillview.table.filters.StringFilterFactory;
import org.hillview.table.filters.StringFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;

public class FindSketch implements ISketch<ITable, FindSketch.Result> {
    public static final class Result implements IJson {
        /**
         * Number of occurrences of the string to find above the current row.
         */
        public final long before;
        /**
         * Number of occurrences of the string to find below the current row.
         */
        public final long after;


        /**
         * First row that matches the string after the top row.
         */
        @Nullable
        public final RowSnapshot firstRow;

        Result() {
            this.before = 0;
            this.after = 0;
            this.firstRow = null;
        }

        Result(long before, long after, @Nullable RowSnapshot firstRow) {
            this.before = before;
            this.after= after;
            this.firstRow = firstRow;
        }

        Result add(Result other, RecordOrder order) {
            @Nullable RowSnapshot fr;
            if (this.firstRow == null) {
                fr = other.firstRow;
            } else if (other.firstRow == null) {
                fr = this.firstRow;
            } else {
                int compare = this.firstRow.compareTo(other.firstRow, order);
                if (compare < 0)
                    fr = this.firstRow;
                else
                    fr = other.firstRow;
            }
            return new Result(this.before + other.before, this.after + other.after, fr);
        }

        @Override
        public JsonElement toJsonTree() {
            JsonObject object = new JsonObject();
            object.addProperty("before", this.before);
            object.addProperty("after", this.after);
            if (this.firstRow == null)
                object.addProperty("firstRow", (String)null);
            else
                object.add("firstRow", this.firstRow.toJsonTree());
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
    private final boolean next;


    public FindSketch(final StringFilterDescription stringFilterDescription,
                      final @Nullable RowSnapshot topRow, final RecordOrder recordOrder,
                      final boolean excludeTopRow, final boolean next) {
        this.stringFilterDescription = stringFilterDescription;
        this.topRow = topRow;
        this.recordOrder = next ? recordOrder: recordOrder.reverse();
        this.excludeTopRow = true;
        this.next = next;
    }

    public FindSketch(final StringFilterDescription stringFilterDescription,
                      final @Nullable RowSnapshot topRow,
                      final RecordOrder recordOrder, final boolean excludeTopRow) {
        this.stringFilterDescription = stringFilterDescription;
        this.topRow = topRow;
        this.recordOrder = recordOrder;
        this.excludeTopRow = excludeTopRow;
        this.next = true;
    }

    public FindSketch(final StringFilterDescription stringFilterDescription,
                      final @Nullable RowSnapshot topRow,
                      final RecordOrder recordOrder) {
        this(stringFilterDescription, topRow, recordOrder, false, true);
        }

    @Override
    public Result create(ITable data) {
        long before = 0;
        long after = 0;
        IRowIterator rowIt = data.getRowIterator();
        Schema toCheck = this.recordOrder.toSchema();
        IStringFilter stringFilter = StringFilterFactory.getFilter(this.stringFilterDescription);
        assert(stringFilter != null);
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data, toCheck);
        VirtualRowSnapshot smallestMatch = new VirtualRowSnapshot(data, toCheck);

        for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
            vw.setRow(i);
            if (!vw.matches(stringFilter))
                continue;
            boolean match_before;
            if (this.topRow == null)
                match_before = false;
            else
                match_before = (this.topRow.compareTo(vw, this.recordOrder) > 0) ||
                        ((this.topRow.compareTo(vw, this.recordOrder) == 0) && this.excludeTopRow);
            if (match_before)
                before += 1;
            else {
                if (after == 0) {
                    smallestMatch.setRow(i);
                } else {
                    if (smallestMatch.compareTo(vw, this.recordOrder) > 0)
                        smallestMatch.setRow(i);
                }
                after += 1;
            }
        }
        RowSnapshot firstRow;
        if (after == 0) {
            firstRow = null;
        } else {
            firstRow = smallestMatch.materialize();
        }
        if(!next) {
            long tmp = before;
            before = after;
            after = tmp;
        }
        return new Result(before, after, firstRow);
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

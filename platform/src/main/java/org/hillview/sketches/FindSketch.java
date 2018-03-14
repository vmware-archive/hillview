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
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class FindSketch implements ISketch<ITable, FindSketch.Result> {
    public static final class Result implements IJson {
        /**
         * Number of occurrences of the string to find.
         */
        public final long count;
        /**
         * First row that matches the string after the top row.
         */
        @Nullable
        public final RowSnapshot firstRow;

        Result() {
            this.count = 0;
            this.firstRow = null;
        }

        Result(long count, @Nullable RowSnapshot firstRow) {
            this.count = count;
            this.firstRow = firstRow;
        }

        public Result add(Result other, RecordOrder order) {
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
            return new Result(this.count + other.count, fr);
        }

        @Override
        public JsonElement toJsonTree() {
            JsonObject object = new JsonObject();
            object.addProperty("count", this.count);
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
    private final StringFilterDescription toFind;
    /**
     * Only return rows larger than this row.
     * If this is null there are no constraints.
     */
    @Nullable
    private final RowSnapshot topRow;
    /**
     * Order used for sorting data.
     */
    private final RecordOrder recordOrder;

    public FindSketch(final StringFilterDescription toFind,
                      final @Nullable RowSnapshot topRow,
                      final RecordOrder recordOrder) {
        this.toFind = toFind;
        this.topRow = topRow;
        this.recordOrder = recordOrder;
    }

    @Override
    public Result create(ITable data) {
        long count = 0;
        IStringFilter filter = this.toFind.getFilter();
        IRowIterator rowIt = data.getRowIterator();
        Schema toCheck = this.recordOrder.toSchema();
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data, toCheck);
        VirtualRowSnapshot smallestMatch = new VirtualRowSnapshot(data, toCheck);

        for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
            vw.setRow(i);
            if (!vw.matches(filter))
                continue;
            count++;
            if ((this.topRow != null) && (this.topRow.compareTo(vw, this.recordOrder) > 0)) {
                // This matches, but is before the topRow.
                continue;
            }

            if (!smallestMatch.exists()) {
                smallestMatch.setRow(i);
            } else {
                if (smallestMatch.compareTo(vw, this.recordOrder) > 0)
                    smallestMatch.setRow(i);
            }
        }

        RowSnapshot firstRow;
        if (!smallestMatch.exists()) {
            firstRow = null;
        } else {
            firstRow = smallestMatch.materialize();
        }
        return new Result(count, firstRow);
    }

    @Nullable
    @Override
    public Result zero() {
        return new Result();
    }

    @Nullable
    @Override
    public Result add(@Nullable Result left, @Nullable Result right) {
        return Converters.checkNull(left).add(Converters.checkNull(right), this.recordOrder);
    }
}

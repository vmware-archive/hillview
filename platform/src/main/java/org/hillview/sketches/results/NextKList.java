/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.hillview.dataset.api.IJsonSketchResult;
import org.hillview.table.AggregateDescription;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The data structure used to store the next K rows in a Table from a given starting point (topRow)
 * according to a RecordSortOrder.
 */
public class NextKList implements IJsonSketchResult {
    static final long serialVersionUID = 1;
    
    /**
     * This table has one row for each row displayed.  Only the visible columns
     * are contained.
     */
    public final SmallTable rows;
    /**
     * This table has one row for each row displayed and one column for each
     * aggregated column.  It may be missing if no columns are aggregated.
     */
    @Nullable
    public final SmallTable aggregates;
    /**
     * The number of times each row in the above table occurs in the original DataSet.
     */
    public final IntList count;
    /**
     * The row number of the starting point (topRow)
     */
    public final long startPosition;
    /**
     * The number of rows the statistics are computed over. For MG or Exact, this
     * equals the number of rows in the input tuple. Whereas for sample heavy
     * hitters, it is the number of samples.
     */
    public final long rowsScanned;

    public NextKList(SmallTable rows, @Nullable SmallTable aggregates,
                     IntList count, long position, long rowsScanned) {
        this.rows = rows;
        this.aggregates = aggregates;
        this.count = count;
        this.startPosition = position;
        this.rowsScanned = rowsScanned;
        /* If the table is empty, discard the counts. Else check we have counts for each row.*/
        if ((rows.getNumOfRows() != 0) && (count.size() != rows.getNumOfRows()))
            throw new IllegalArgumentException("Mismatched table and count length");
    }

    /**
     * We also use a NextKList to filter and display the result of a FreqKList.The start position is
     * not meaningful in this context and is set to 0.
     * @param listRows List of RowSnapshots from a FreqKList
     * @param listCounts List of Counts from a FreqKList
     * @param schema The schema of the RowSnapshots
     * @param rowsScanned The number of rows the statistics are computed over.
     */
    public NextKList(List<RowSnapshot> listRows, IntList listCounts, Schema schema, long rowsScanned) {
        this.rows = new SmallTable(schema, listRows);
        this.aggregates = null;
        this.count = listCounts;
        this.startPosition = 0;
        this.rowsScanned = rowsScanned;
    }

    public boolean isEmpty() {
        return this.rows.getNumOfRows() == 0;
    }

    // Computes a schema suitable for the columns in aggregates.
    // This schema is never displayed to the user, so it's not very important.
    public static Schema getSchema(AggregateDescription[] aggregates) {
        Schema schema = new Schema();
        for (AggregateDescription cad : aggregates) {
            String name = schema.newColumnName(cad.cd.name);
            // aggregate columns always contain doubles
            ColumnDescription cd = new ColumnDescription(name, ContentsKind.Double);
            schema.append(cd);  // to generate different names
        }
        return schema;
    }

    /**
     * A NextK list containing an empty table with the specified schema.
     */
    public NextKList(Schema schema, @Nullable AggregateDescription[] aggregates) {
        this.rows = new SmallTable(schema);
        if (aggregates != null) {
            this.aggregates = new SmallTable(getSchema(aggregates));
        } else {
            this.aggregates = null;
        }
        this.count = IntLists.EMPTY_LIST;
        this.startPosition = 0;
        this.rowsScanned = 0;
    }

    public String toLongString(int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.rows.toString());
        builder.append(System.getProperty("line.separator"));
        IRowIterator rowIt = this.rows.getRowIterator();
        int nextRow = rowIt.getNextRow();
        int i = 0;
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(this.rows);
        while ((nextRow >= 0) && (i < rowsToDisplay)) {
            vrs.setRow(nextRow);
            builder.append(vrs.toString()).append(": ").append(this.count.getInt(i));
            builder.append(System.getProperty("line.separator"));
            nextRow = rowIt.getNextRow();
            i++;
        }
        if (i == rowsToDisplay)
            builder.append("...");

        if (this.aggregates != null) {
            builder.append(this.aggregates.toString());
            builder.append(System.getProperty("line.separator"));
            rowIt = this.aggregates.getRowIterator();
            nextRow = rowIt.getNextRow();
            i = 0;
            vrs = new VirtualRowSnapshot(this.aggregates);
            while ((nextRow >= 0) && (i < rowsToDisplay)) {
                vrs.setRow(nextRow);
                builder.append(vrs.toString());
                builder.append(System.getProperty("line.separator"));
                nextRow = rowIt.getNextRow();
                i++;
            }
            if (i == rowsToDisplay)
                builder.append("...");
        }

        return builder.toString();
    }

    @Override
    public String toString() {
        return this.toLongString(20);
    }

    @Override
    public JsonElement toJsonTree() {
        // The result looks like a TableDataView typescript class
        JsonObject result = new JsonObject();
        result.addProperty("rowsScanned", this.rowsScanned);
        result.addProperty("startPosition", this.startPosition);
        JsonArray rows = new JsonArray();
        result.add("rows", rows);
        for (int i = 0; i < this.count.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("count", this.count.getInt(i));
            row.add("values", new RowSnapshot(this.rows, i).toJsonTree());
            rows.add(row);
        }
        if (this.aggregates == null)
            result.add("aggregates", null);
        else {
            JsonArray aggregates = new JsonArray();
            for (int i = 0; i < this.aggregates.getNumOfRows(); i++) {
                RowSnapshot aggRow = new RowSnapshot(this.aggregates, i);
                aggregates.add(aggRow.toJsonTree());
            }
            result.add("aggregates", aggregates);
        }
        return result;
    }
}

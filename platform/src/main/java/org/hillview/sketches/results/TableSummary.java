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

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;
import org.hillview.table.Schema;

import javax.annotation.Nullable;

/**
 * Describes the schema of a table and the number of rows.
 */
public class TableSummary implements IJson {
    static final long serialVersionUID = 1;
    
    // The sketch zero() element can be produced without looking at the data at all.
    // So we need a way to represent a "zero" schema.  An empty schema is in principle
    // legal for a table, so we use a null to represent a yet "unknown" schema.
    @Nullable
    public final Schema schema;
    public final long   rowCount;

    public TableSummary(@Nullable Schema schema, long rowCount) {
        this.schema = schema;
        this.rowCount = rowCount;
    }

    public TableSummary() {
        this.schema = null;
        this.rowCount = 0;
    }

    public TableSummary add(TableSummary other) {
        @Nullable Schema s = this.schema;
        if (this.schema == null)
            s = other.schema;
        else if (other.schema != null && !this.schema.equals(other.schema))
            throw new RuntimeException("Schemas differ:\n" +
                this.schema.diff(other.schema));
        return new TableSummary(s, this.rowCount + other.rowCount);
    }

    @Override
    public JsonElement toJsonTree() {
        JsonObject result = new JsonObject();
        result.addProperty("rowCount", this.rowCount);
        if (this.schema == null)
            result.add("schema", JsonNull.INSTANCE);
        else
            result.add("schema", this.schema.toJsonTree());      
        return result;
    }

    @Override
    public String toString() {
        return "" + "Rows: " + this.rowCount +
                ", schema: " + (this.schema != null ? this.schema.toString() : "null");
    }
}

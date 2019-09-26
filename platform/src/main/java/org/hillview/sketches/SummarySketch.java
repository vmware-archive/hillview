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

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A sketch which retrieves the Schema and size of a distributed table.
 * Two schemas can be added only if they are identical.
 * We use a null to represent a "zero" for the schemas.
 * (This Sketch is logically a ConcurrentSketch combining
 * an OptionMonoid[Schema] sketch and integer addition).
 */
public class SummarySketch implements ISketch<ITable, SummarySketch.TableSummary> {
    public static class TableSummary implements IJson {
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

        TableSummary() {
            this.schema = null;
            this.rowCount = 0;
        }

        TableSummary add(TableSummary other) {
            @Nullable Schema s = null;
            if (this.schema == null)
                s = other.schema;
            else if (other.schema == null)
                s = this.schema;
            else if (!this.schema.equals(other.schema))
                throw new RuntimeException("Schemas differ");
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
    }

    @Override @Nullable
    public TableSummary zero() {
        return new TableSummary();
    }

    @Override @Nullable
    public TableSummary add(@Nullable TableSummary left, @Nullable TableSummary right) {
        assert left != null;
        assert right != null;
        return left.add(right);
    }

    @Override
    public TableSummary create(@Nullable ITable data) {
        Converters.checkNull(data);
        return new TableSummary(data.getSchema(), data.getNumOfRows());
    }
}

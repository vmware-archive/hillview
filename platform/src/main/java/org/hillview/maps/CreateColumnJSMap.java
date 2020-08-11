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

package org.hillview.maps;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseColumn;
import org.hillview.table.columns.IntervalColumn;
import org.hillview.table.rows.JSVirtualRowSnapshot;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;

/**
 * This map creates a new column by running a JavaScript
 * function over a set of columns.
 */
public class CreateColumnJSMap extends AppendColumnMap {
    static final long serialVersionUID = 1;

    public static class Info implements Serializable {
        /**
         * A JavaScript function named 'map' that computes the value in the new column.
         * It has a single input, which is a row in the table, and it produces a single output,
         * a value that is written to the destination column.
         */
        String jsFunction;
        Schema schema;
        String outputColumn;
        ContentsKind outputKind;
        /**
         * Map string->string described by a string array.
         */
        @Nullable
        String[] renameMap;

        public Info(String jsFunction, Schema schema, String outputColumn, ContentsKind outputKind, @Nullable String[] renameMap) {
            this.jsFunction = jsFunction;
            this.schema = schema;
            this.outputColumn = outputColumn;
            this.outputKind = outputKind;
            this.renameMap = renameMap;
        }
    }

    public final Info info;

    public CreateColumnJSMap(Info info) {
        super(info.outputColumn, -1);
        this.info = info;
    }

    @Override
    IColumn createColumn(ITable table) {
        try {
            HashMap<String, String> renameMap = Utilities.arrayToMap(this.info.renameMap);
            Context context = Context.newBuilder().allowAllAccess(true).build();
            // Compiles the JS function
            context.eval("js", this.info.jsFunction);

            ColumnDescription outCol = new ColumnDescription(this.info.outputColumn, this.info.outputKind);
            IMutableColumn col;
            IMutableColumn endCol = null;  // only used for Intervals
            ContentsKind kind = this.info.outputKind;
            IMembershipSet set = table.getMembershipSet();
            if (kind == ContentsKind.Interval) {
                ColumnDescription cd = new ColumnDescription("start", ContentsKind.Double);
                col = BaseColumn.create(cd, set.getMax(), set.getSize());
                endCol = BaseColumn.create(cd, set.getMax(), set.getSize());
            } else {
                col = BaseColumn.create(outCol, set.getMax(), set.getSize());
            }
            table.getLoadedColumns(this.info.schema.getColumnNames());

            JSVirtualRowSnapshot vrs = new JSVirtualRowSnapshot(
                    table, this.info.schema, renameMap, context);
            ProxyObject vrsProxy = ProxyObject.fromMap(vrs);
            IRowIterator it = table.getMembershipSet().getIterator();
            int r = it.getNextRow();
            Value function = context.eval("js", "vrs => map(vrs)");
            assert function.canExecute();
            while (r >= 0) {
                vrs.setRow(r);
                Value value = function.execute(vrsProxy);
                if (value == null)
                    col.setMissing(r);
                else {
                    switch (kind) {
                        case None:
                            throw new RuntimeException("Only null values can be stored in this column");
                        case String:
                        case Json:
                            col.set(r, value.toString());
                            break;
                        case Date:
                        case Time:
                            double timestampLocal = value.invokeMember("getTime").asDouble();
                            col.set(r, timestampLocal);
                            break;
                        case LocalDate:
                            double ts = value.invokeMember("getTime").asDouble();
                            // ts is the local time; we have to adjust for the timezone
                            double offset = value.invokeMember("getTimezoneOffset").asDouble();
                            col.set(r, ts - offset * 60 * 1000);
                            break;
                        case Integer:
                            col.set(r, value.asInt());
                            break;
                        case Double:
                            col.set(r, value.asDouble());
                            break;
                        case Duration:
                            // TODO
                            break;
                        case Interval:
                            Value v0 = value.getArrayElement(0);
                            Value v1 = value.getArrayElement(1);
                            col.set(r, v0.asDouble());
                            assert endCol != null;
                            endCol.set(r, v1.asDouble());
                            break;
                        default:
                            throw new RuntimeException("Unhandled kind " + kind);
                    }
                }
                r = it.getNextRow();
            }

            if (kind == ContentsKind.Interval)
                return new IntervalColumn(outCol, col, endCol);
            return col.seal();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}

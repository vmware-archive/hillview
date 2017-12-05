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

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.table.rows.JSVirtualRowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.Instant;

/**
 * This map creates a new column by running a JavaScript
 * function over a set of columns.
 */
public class CreateColumnJSMap extends AddColumnMap {
    /**
     * A JavaScript function named 'map' that computes the value in the new column.
     * It has a single input, which is a row in the table, and it produces a single output,
     * a value that is written to the destination column.
     */
    public final String jsFunction;
    /**
     * Set of columns that the function can access.
     */
    public final Schema inputColumns;
    /**
     * Description of the output column to produce.
     */
    public final ColumnDescription outputColumn;

    public CreateColumnJSMap(String jsFunction, Schema inputColumns, ColumnDescription outputColumn) {
        super(outputColumn.name, -1);
        this.jsFunction = jsFunction;
        this.inputColumns = inputColumns;
        this.outputColumn = outputColumn;
    }

    @Override
    IColumn createColumn(ITable table) {
        try {
            ScriptEngineManager factory = new ScriptEngineManager();
            ScriptEngine engine = factory.getEngineByName("nashorn");
            // Compiles the JS function
            engine.eval(this.jsFunction);
            Invocable invocable = (Invocable)engine;

            IAppendableColumn col = BaseListColumn.create(this.outputColumn);
            // TODO: ensure that the input columns are loaded.
            ContentsKind kind = this.outputColumn.kind;

            JSVirtualRowSnapshot vrs = new JSVirtualRowSnapshot(table, engine);
            IRowIterator it = table.getMembershipSet().getIterator();
            int r = it.getNextRow();
            while (r >= 0) {
                vrs.setRow(r);
                Object value = invocable.invokeFunction("map", vrs);
                if (value == null)
                    col.appendMissing();
                else {
                    switch (kind) {
                        case None:
                            col.append(value);
                        case Category:
                        case String:
                        case Json:
                            col.append((String)value);
                            break;
                        case Date:
                            ScriptObjectMirror jsDate = (ScriptObjectMirror)value;
                            double timestampLocal = (double)jsDate.callMember("getTime");
                            Instant instant = Converters.toDate(timestampLocal);
                            col.append(instant);
                            break;
                        case Integer:
                            col.append((int)(double)value);
                            break;
                        case Double:
                            col.append((double)value);
                            break;
                        case Duration:
                            // TODO
                            col.append(value);
                            break;
                    }
                }
                r = it.getNextRow();
            }
            return col;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}

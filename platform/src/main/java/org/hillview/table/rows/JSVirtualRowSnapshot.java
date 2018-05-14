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

package org.hillview.table.rows;

import jdk.nashorn.api.scripting.JSObject;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;

/**
 * This class is an adaptor around the VirtualRowSnapshot which is used by the JavaScript
 * Nashorn engine.  When it retrieves an Instant, it gets a JavaScript date instead.
 */
public class JSVirtualRowSnapshot extends VirtualRowSnapshot {
    private final ScriptEngine engine;

    public JSVirtualRowSnapshot(
            ITable table, Schema schema,
            @Nullable
            HashMap<String, String> columnRenameMap,
            ScriptEngine engine) {
        super(table, schema, columnRenameMap);
        this.engine = engine;
    }

    @Override
    @Nullable
    public Object get(Object key) {
        IColumn col = this.getColumnChecked((String)key);
        if (col.getDescription().kind == ContentsKind.Date) {
            double dateEncoding = super.getDouble((String)key);
            // https://stackoverflow.com/questions/33110942/supply-javascript-date-to-nashorn-script
            try {
                //noinspection RedundantCast
                return (JSObject) this.engine.eval("new Date(" + Double.toString(dateEncoding) + ")");
            } catch (ScriptException e) {
                throw new RuntimeException(e);
            }
        }

        Object result = super.get(key);
        if (result == null)
            return null;
        return result;
    }
}

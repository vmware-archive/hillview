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

import org.graalvm.polyglot.Context;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import java.util.HashMap;

/**
 * This class is an adaptor around the VirtualRowSnapshot which is used by the JavaScript
 * engine.  When it retrieves an Instant, it gets a JavaScript date instead.
 */
public class JSVirtualRowSnapshot extends VirtualRowSnapshot {
    static final long serialVersionUID = 1;

    private final Context engine;

    public JSVirtualRowSnapshot(
            ITable table, Schema schema,
            @Nullable
            HashMap<String, String> columnRenameMap,
            Context engine) {
        super(table, schema, columnRenameMap);
        this.engine = engine;
    }

    @Override
    @Nullable
    public Object get(Object key) {
        IColumn col = this.getColumnChecked((String)key);
        ContentsKind kind = col.getKind();
        String k = (String)key;
        if (kind == ContentsKind.Date || kind == ContentsKind.Time) {
            double dateEncoding = super.getDouble(k);
            return this.engine.eval("js","new Date(" + dateEncoding + ")");
        } else if (kind == ContentsKind.LocalDate) {
            double dateEncoding = super.getDouble(k);
            return this.engine.eval("js","new Date(" + dateEncoding + " + (new Date(0).getTimezoneOffset() * 60000))");
        } else if (kind == ContentsKind.Interval) {
            double start = super.getEndpoint(k, true);
            double end = super.getEndpoint(k, false);
            return new double[] { start, end };
        }
        return super.get(key);
    }
}

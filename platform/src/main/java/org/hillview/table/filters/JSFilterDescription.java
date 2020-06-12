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

package org.hillview.table.filters;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.ITableFilterDescription;
import org.hillview.table.rows.VirtualRowSnapshot;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A filter that uses JavaScript to filter data.
 */
public class JSFilterDescription implements ITableFilterDescription {
    static final long serialVersionUID = 1;
    
    private final Schema schema;
    private final String jsCode;
    @Nullable private final Map<String, String> renameMap;
    /**
     * Make a filter that accepts rows that compares in the specific way with the
     * given row.
     * @param jsCode Javascript function called 'filter' consuming
     *               a row and returning a boolean.
     * @param comparedColumns  Columns that may participate in the comparison.
     * @param renameMap        Map that shows how columns were renamed in the UI.
     */
    public JSFilterDescription(
            String jsCode,
            Schema comparedColumns, @Nullable Map<String, String> renameMap) {
        this.jsCode = jsCode;
        this.schema = comparedColumns;
        this.renameMap = renameMap;
    }

    class JSFilter implements ITableFilter {
        private final VirtualRowSnapshot vrs;
        private final Value function;
        private final ProxyObject vrsProxy;

        JSFilter(ITable table) {
            try {
                this.vrs = new VirtualRowSnapshot(
                        table, JSFilterDescription.this.schema,
                        JSFilterDescription.this.renameMap);
                this.vrsProxy = ProxyObject.fromMap(this.vrs);
                Context context = Context.newBuilder().allowAllAccess(true).build();
                // Compiles the JS function
                context.eval("js", JSFilterDescription.this.jsCode);
                this.function = context.eval("js", "vrs => filter(vrs)");
                assert this.function.canExecute();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public boolean test(int rowIndex) {
            this.vrs.setRow(rowIndex);
            return this.function.execute(this.vrsProxy).asBoolean();
        }
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        return new JSFilter(table);
    }
}

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
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;

/**
 * A filter that uses JavaScript to filter data.
 */
public class JSFilterDescription implements ITableFilterDescription {
    static final long serialVersionUID = 1;

    public static class Info implements Serializable {
        Schema schema;
        String jsCode;
        @Nullable String[] renameMap;

        public Info(Schema schema, String jsCode, @Nullable String[] renameMap) {
            this.schema = schema;
            this.jsCode = jsCode;
            this.renameMap = renameMap;
        }
    }

    private final Info info;

    /**
     * Make a filter that accepts rows that compares in the specific way with the
     * given row.
     */
    public JSFilterDescription(Info info) {
        this.info = info;
    }

    class JSFilter implements ITableFilter {
        private final VirtualRowSnapshot vrs;
        private final Value function;
        private final ProxyObject vrsProxy;

        JSFilter(ITable table) {
            HashMap<String, String> renameMap = Utilities.arrayToMap(JSFilterDescription.this.info.renameMap);
            try {
                this.vrs = new VirtualRowSnapshot(
                        table, JSFilterDescription.this.info.schema,
                        renameMap);
                this.vrsProxy = ProxyObject.fromMap(this.vrs);
                Context context = Context.newBuilder().allowAllAccess(true).build();
                // Compiles the JS function
                context.eval("js", JSFilterDescription.this.info.jsCode);
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

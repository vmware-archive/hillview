/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

package org.hillview;

import org.hillview.table.Schema;
import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.nio.file.Paths;

/**
 * A LazySchema may only contain a Path, loading the schema on demand from the filesystem.
 */
public class LazySchema {
    @Nullable
    Schema schema;
    @Nullable
    String schemaPath;

    public LazySchema() {
        this.schema = null;
        this.schemaPath = null;
    }

    public LazySchema(Schema schema) {
        this.schema = schema;
        this.schemaPath = null;
    }

    public LazySchema(@Nullable String schemaPath) {
        this.schema = null;
        this.schemaPath = schemaPath;
    }

    @Nullable
    public synchronized Schema getSchema() {
        if (this.schema == null) {
            if (Utilities.isNullOrEmpty(this.schemaPath))
                return null;
            this.schema = Schema.readFromJsonFile(
                    Paths.get(Converters.checkNull(this.schemaPath)));
        }
        return this.schema;
    }

    public boolean isNull() {
        return this.schema == null && Utilities.isNullOrEmpty(this.schemaPath);
    }
}

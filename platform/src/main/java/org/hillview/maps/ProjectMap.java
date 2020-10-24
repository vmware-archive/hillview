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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * A map that preserves the specified columns.
 * If a column is requested that does not exist, an empty column is created.
 */
public class ProjectMap implements IMap<ITable, ITable> {
    static final long serialVersionUID = 1;
    private final Schema schema;

    public ProjectMap(Schema schema) {
        this.schema = schema;
    }

    @Nullable
    @Override
    public ITable apply(@Nullable ITable data) {
        assert data != null;
        return data.project(this.schema);
    }
}

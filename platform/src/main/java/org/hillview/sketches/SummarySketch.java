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

import org.hillview.dataset.TableSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.TableSummary;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * A sketch which retrieves the Schema and size of a distributed table.
 * Two schemas can be added only if they are identical.
 * We use a null to represent a "zero" for the schemas.
 */
public class SummarySketch implements TableSketch<TableSummary> {
    static final long serialVersionUID = 1;
    
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

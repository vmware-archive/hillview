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

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.*;

/**
 * A class that scans a column and collects basic statistics: maximum, minimum,
 * number of non-empty rows and the moments of asDouble values.
 */
public abstract class ColumnStatistics implements IJson {
    protected long presentCount;
    // Number of missing elements
    protected long missingCount;

    public ColumnStatistics() {
        this.presentCount = 0;
        this.missingCount = 0;
    }

    /**
     * @return the number of non-missing rows in a column
     */
    public long getPresentCount() { return this.presentCount; }

    public long getRowCount() { return this.presentCount + this.missingCount; }

    abstract void createStats(final ColumnAndConverter column,
                              final IMembershipSet membershipSet);

    abstract ColumnStatistics union(ColumnStatistics other);
}

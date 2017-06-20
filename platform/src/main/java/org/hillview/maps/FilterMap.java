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
 *
 */

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.FalseTableFilter;
import org.hillview.table.TableFilter;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;

/**
 * A Map which implements table filtering: given a row index it returns true if the
 * row is in the resulting table.
 */
public class FilterMap implements IMap<ITable, ITable> {
    /**
     * Argument is a row index.
     * Returns true if a row has to be preserved
     */
    private final TableFilter rowFilterPredicate;

    public FilterMap() {
        rowFilterPredicate = new FalseTableFilter();
    }

    public FilterMap(TableFilter rowFilterPredicate) {
        this.rowFilterPredicate = rowFilterPredicate;
    }

    @Override
    public ITable apply(ITable data) {
        this.rowFilterPredicate.setTable(data);
        IMembershipSet result = data.getMembershipSet().filter(this.rowFilterPredicate::test);
        return data.selectRowsFromFullTable(result);
    }
}

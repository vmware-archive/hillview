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

import org.hillview.dataset.api.IMap;
import org.hillview.dataset.api.Pair;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class SetOperationMap implements IMap<Pair<ITable, ITable>, ITable> {
    static final long serialVersionUID = 1;
    private final String operation;

    public SetOperationMap(String operation) {
        this.operation = operation;
    }

    @Override
    public ITable apply(@Nullable Pair<ITable, ITable> data) {
        Converters.checkNull(data);
        Converters.checkNull(data.first);
        Converters.checkNull(data.second);
        IMembershipSet first = data.first.getMembershipSet();
        IMembershipSet second = data.second.getMembershipSet();
        IMembershipSet rows;
        switch (this.operation) {
            case "Union":
                rows = first.union(second);
                break;
            case "Intersection":
                rows = first.intersection(second);
                break;
            case "Replace":
                rows = second;
                break;
            case "Exclude":
                rows = first.setMinus(second);
                break;
            default:
                throw new RuntimeException("Unexpected operation " + this.operation);
        }

        return data.first.selectRowsFromFullTable(rows);
    }
}

/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.targets;

import org.hillview.*;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.SetOperationMap;
import org.hillview.table.api.ITable;

/**
 * This is an RpcTarget that has a reference to an IDataSet[Pair[ITable,ITable]]
 */
// All RpcTarget objects must be public
public final class TablePairTarget extends RpcTarget {
    static final long serialVersionUID = 1;

    private final IDataSet<Pair<ITable, ITable>> tables;

    TablePairTarget(IDataSet<Pair<ITable, ITable>> tables, HillviewComputation computation) {
        super(computation);
        this.tables = tables;
        this.registerObject();
    }

    @HillviewRpc
    public void setOperation(RpcRequest request, RpcRequestContext context) {
        String op = request.parseArgs(String.class);
        SetOperationMap sm = new SetOperationMap(op);
        this.runMap(this.tables, sm, TableTarget::new, request, context);
    }
}

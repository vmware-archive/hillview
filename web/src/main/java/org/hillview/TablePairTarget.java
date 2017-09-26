/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview;

import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.Pair;
import org.hillview.maps.SetOperationMap;
import org.hillview.table.api.ITable;

import javax.websocket.Session;

/**
 * This is a remote object that has a reference to an IDataSet[Pair[ITable,ITable]]
 */
class TablePairTarget extends RpcTarget {
    private final IDataSet<Pair<ITable, ITable>> tables;
    TablePairTarget(IDataSet<Pair<ITable, ITable>> tables) {
        this.tables = tables;
    }

    @HillviewRpc
    void setOperation(RpcRequest request, Session session) {
        String op = request.parseArgs(String.class);
        SetOperationMap sm = new SetOperationMap(op);
        this.runMap(this.tables, sm, TableTarget::new, request, session);
    }
}

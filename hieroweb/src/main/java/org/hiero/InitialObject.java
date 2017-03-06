/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hiero;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;

import javax.websocket.Session;
import java.util.ArrayList;
import java.util.List;

public class InitialObject extends RpcTarget {
    @HieroRpc
    void loadTable(RpcRequest request, Session session) {
        // TODO: look at request.  Now we just supply always the same table
        Table t = Table.testTable();
        final int parts = 5;
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        for (int i = 0; i < parts; i++) {
            LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
            fragments.add(data);
        }
        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments);
        TableTarget table = new TableTarget(big);
        RpcReply reply = request.createReply(table.idToJson());
        reply.send(session);
        request.closeSession(session);
    }

    @Override
    public String toString() {
        return "Initial object, " + super.toString();
    }
}

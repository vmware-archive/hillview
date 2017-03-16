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

import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.api.ITable;

import javax.websocket.Session;

public final class TableTarget extends RpcTarget {
    private final IDataSet<ITable> table;
    TableTarget(IDataSet<ITable> table) {
        this.table = table;
    }

    static class HistoArgs {
        String columnName = "";
        int    min;
        int    max;
    }

    @HieroRpc
    void getSchema(RpcRequest request, Session session) {
        SummarySketch ss = new SummarySketch();
        this.runSketch(this.table, ss, request, session);
    }

    @HieroRpc
    void getTableView(RpcRequest request, Session session) {
        RecordOrder ro = gson.fromJson(request.arguments, RecordOrder.class);
        NextKSketch nk = new NextKSketch(ro, null, 10);
        this.runSketch(this.table, nk, request, session);
    }

    @HieroRpc
    void histogram(RpcRequest request, Session session) {
        HistoArgs info = gson.fromJson(request.arguments, HistoArgs.class);
        // TODO: compute number of buckets
        BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(info.min, info.max, 40);
        Hist1DSketch sk = new Hist1DSketch(buckets, info.columnName, null);
        this.runSketch(this.table, sk, request, session);
    }

    @HieroRpc
    void range(RpcRequest request, Session session) {
        String column = gson.fromJson(request.arguments, String.class);
        // TODO: create a string converter if necessary
        BasicColStatSketch sk = new BasicColStatSketch(column, null, 0, 1.0);
        this.runSketch(this.table, sk, request, session);
    }

    @Override
    public String toString() {
        return "TableTarget object, " + super.toString();
    }
}

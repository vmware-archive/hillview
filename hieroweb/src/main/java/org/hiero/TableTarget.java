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
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.dataset.api.PartialResultMonoid;
import org.hiero.sketch.spreadsheet.SummarySketch;
import org.hiero.sketch.table.RecordOrder;
import org.hiero.sketch.table.api.ITable;
import rx.Observable;

import javax.websocket.Session;

import static org.hiero.RpcObjectManager.gson;

public class TableTarget extends RpcTarget {
    protected final IDataSet<ITable> table;

    public TableTarget(IDataSet<ITable> table) {
        this.table = table;
    }

    @HieroRpc
    void getSchema(RpcRequest request, Session session) {
        SummarySketch ss = new SummarySketch();
        Observable<PartialResult<SummarySketch.TableSummary>> sketches = this.table.sketch(ss);
        PartialResultMonoid<SummarySketch.TableSummary> prm =
                new PartialResultMonoid<SummarySketch.TableSummary>(ss);
        Observable<PartialResult<SummarySketch.TableSummary>> accum = sketches.scan(prm::add);
        ResultObserver<SummarySketch.TableSummary> ro =
                new ResultObserver<SummarySketch.TableSummary>(request, session);
        accum.subscribe(ro);
    }

    @HieroRpc
    void getTableView(RpcRequest request, Session session) {
        RecordOrder ro = gson.fromJson(request.arguments, RecordOrder.class);
        // TODO
    }

    @Override
    public String toString() {
        return "TableTarget object, " + super.toString();
    }
}

/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import {Groups, HistogramRequestInfo, IColumnDescription, RemoteObjectId, SampleSet} from "../javaBridge";
import {ChartView} from "./chartView";
import {FullPage, PageTitle} from "../ui/fullPage";
import {ICancellable, PartialResult} from "../util";
import {DisplayName} from "../schemaClass";
import {BaseReceiver} from "../tableTarget";
import {RpcRequest} from "../rpc";
import {CommonArgs, ReceiverCommon} from "../ui/receiver";

export class CorrelationHeatmapView extends ChartView<Groups<Groups<number>>[]> {
    constructor(args: CommonArgs, page: FullPage) {
        super(args.remoteObjectId.remoteObjectId, args.rowCount, args.schema, page, "CorrelationHeatmaps")
    }

    protected export(): void {
        // TODO
    }

    protected getCombineRenderer(title: PageTitle): (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return function (p1: FullPage, p2: ICancellable<RemoteObjectId>) {
            return null;
        };
    }

    protected onMouseMove(): void {
        // TODO
    }

    refresh(): void {
        // TODO
    }

    resize(): void {
        // TODO
    }

    protected showTrellis(colName: DisplayName): void {}

    updateView(data: Groups<Groups<number>>[]) {
        // TODO
    }
}

export class CorrelationHeatmapReceiver extends ReceiverCommon<Groups<Groups<number>>[]> {
    protected view: CorrelationHeatmapView;

    constructor(common: CommonArgs, histoArgs: HistogramRequestInfo[], cds: IColumnDescription[],
                operation: RpcRequest<PartialResult<Groups<Groups<number>>[]>>) {
        super(common, operation, "correlations")
        this.view = new CorrelationHeatmapView(this.args, this.page);
        this.page.setDataView(this.view);
    }

    public onNext(value: PartialResult<Groups<Groups<number>>[]>): void {
        super.onNext(value);
        if (value == null)
            return;
        this.view.updateView(value.data);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}
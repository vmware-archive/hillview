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

/*
 * This file contains lots of classes for accessing the remote TableTarget.java class.
 */

import {RpcRequest, RemoteObject, OnCompleteRenderer} from "./rpc";
import {EqualityFilterDescription} from "./dataViews/equalityFilter";
import {ICancellable, Pair, PartialResult, Seed} from "./util";
import {PointSet, Resolution} from "./ui/ui";
import {IDataView} from "./ui/dataview";
import {FullPage} from "./ui/fullPage";
import {
    BasicColStats,
    CombineOperators, CreateColumnInfo, FilterDescription, HeatMap, Histogram, Histogram3DArgs, HistogramArgs, HLogLog,
    IColumnDescription, NextKList, RangeInfo, RecordOrder, RemoteObjectId, Schema, TableSummary, TopList
} from "./javaBridge";
import {Histogram2DArgs} from "./javaBridge";
import {SelectedObject} from "./selectedObject";
import {HeatMapArrayData} from "./dataViews/trellisHeatMapView";

/**
 * This class methods that correspond directly to TableTarget.java methods.
 */
export class RemoteTableObject extends RemoteObject {
    /**
     * Create a reference to a remote table target.
     * @param {RemoteObjectId} remoteObjectId   Id of remote table on the web server.
     * @param originalTableId                   Id of the table that this one is derived from.
     */
    constructor(remoteObjectId: RemoteObjectId, public originalTableId: RemoteObjectId) {
        super(remoteObjectId);
    }

    public createRangeRequest(r: RangeInfo): RpcRequest<PartialResult<BasicColStats>> {
        return this.createStreamingRpcRequest<BasicColStats>("range", r);
    }

    public createZipRequest(r: RemoteObject): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("zip", r.remoteObjectId);
    }

    public createQuantileRequest(rowCount: number, o: RecordOrder, position: number):
            RpcRequest<PartialResult<any[]>> {
        return this.createStreamingRpcRequest<any[]>("quantile", {
            precision: 100,
            tableSize: rowCount,
            order: o,
            position: position,
            seed: Seed.instance.get()
        });
    }

    public createNextKRequest(order: RecordOrder, firstRow: any[]):
        RpcRequest<PartialResult<NextKList>> {
        let nextKArgs = {
            order: order,
            firstRow: firstRow,
            rowsOnScreen: Resolution.tableRowsOnScreen
        };
        return this.createStreamingRpcRequest<NextKList>("getNextK", nextKArgs);
    }

    public createGetSchemaRequest(): RpcRequest<PartialResult<TableSummary>> {
        return this.createStreamingRpcRequest<TableSummary>("getSchema", null);
    }

    public createHLogLogRequest(colName: string) : RpcRequest<PartialResult<HLogLog>> {
    	return this.createStreamingRpcRequest<HLogLog>("hLogLog",
            { columnName: colName, seed: Seed.instance.get() });
    }

    public createRange2DRequest(r1: RangeInfo, r2: RangeInfo):
    RpcRequest<PartialResult<Pair<BasicColStats, BasicColStats>>> {
        return this.createStreamingRpcRequest<Pair<BasicColStats, BasicColStats>>("range2D", [r1, r2]);
    }

    public createRange2DColsRequest(c1: string, c2: string):
            RpcRequest<PartialResult<Pair<BasicColStats, BasicColStats>>> {
        let r1: RangeInfo = new RangeInfo(c1, null);
        let r2: RangeInfo = new RangeInfo(c2, null);
        return this.createRange2DRequest(r1, r2);
    }

    public createHeavyHittersRequest(columns: IColumnDescription[],
                                     percent: number,
                                     totalRows: number,
                                     isMG: boolean): RpcRequest<PartialResult<TopList>> {
        if (isMG) {
            return this.createStreamingRpcRequest<TopList>("heavyHittersMG",
                {columns: columns, amount: percent,
                    totalRows: totalRows, seed: Seed.instance.get() });
        } else {
            return this.createStreamingRpcRequest<TopList>("heavyHitters",
                {columns: columns, amount: percent,
                    totalRows: totalRows, seed: Seed.instance.get() });
        }
    }

    public createCheckHeavyRequest(r: RemoteObject, schema: Schema):
            RpcRequest<PartialResult<TopList>> {
        return this.createStreamingRpcRequest<TopList>("checkHeavy", {
            hittersId: r.remoteObjectId,
            schema: schema
        });
    }

    public createProjectToEigenVectorsRequest(r: RemoteObject, dimension: number, projectionName: string):
    RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("projectToEigenVectors", {
            id: r.remoteObjectId,
            numComponents: dimension,
            projectionName: projectionName
        });
    }

    public createFilterEqualityRequest(filter: EqualityFilterDescription):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterEquality", filter);
    }

    public createCorrelationMatrixRequest(columnNames: string[], totalRows: number, toSample: boolean):
RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("correlationMatrix", {
            columnNames: columnNames,
            totalRows: totalRows,
            seed: Seed.instance.get(),
            toSample: toSample
        });
    }

    public createCreateColumnRequest(c: CreateColumnInfo): RpcRequest<PartialResult<string>> {
        return this.createStreamingRpcRequest<string>("createColumn", c);
    }

    public createFilterRequest(f: FilterDescription): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterRange", f);
    }

    public createFilter2DRequest(xRange: FilterDescription, yRange: FilterDescription):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filter2DRange", {first: xRange, second: yRange});
    }

    public createHistogram2DMapRequest(info: Histogram2DArgs): RpcRequest<PartialResult<Pair<HeatMap, Histogram>>> {
        return this.createStreamingRpcRequest<Pair<HeatMap, Histogram>>("histogram2D", info);
    }

    public createHeatMapRequest(info: Histogram2DArgs): RpcRequest<PartialResult<HeatMap>> {
        return this.createStreamingRpcRequest<HeatMap>("heatMap", info);
    }

    public createHeatMap3DRequest(info: Histogram3DArgs):
            RpcRequest<PartialResult<HeatMapArrayData>> {
        return this.createStreamingRpcRequest<HeatMapArrayData>("heatMap3D", info);
    }

    public createHistogramRequest(info: HistogramArgs):
            RpcRequest<PartialResult<Pair<Histogram, Histogram>>> {
        return this.createStreamingRpcRequest<Pair<Histogram, Histogram>>("histogram", info);
    }

    public createSetOperationRequest(setOp: CombineOperators): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("setOperation", CombineOperators[setOp]);
    }

    public createSampledControlPointsRequest(rowCount: number, numSamples: number, columnNames: string[]):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("sampledControlPoints",
            {rowCount: rowCount, numSamples: numSamples, columnNames: columnNames, seed: Seed.instance.get() });
    }

    public createCategoricalCentroidsControlPointsRequest(categoricalColumnName: string, numericalColumnNames: string[]):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("categoricalCentroidsControlPoints", {
                categoricalColumnName: categoricalColumnName,
                numericalColumnNames: numericalColumnNames} );
    }

    public createMDSProjectionRequest(id: RemoteObjectId): RpcRequest<PartialResult<PointSet>> {
        return this.createStreamingRpcRequest<PointSet>(
            "makeMDSProjection", { id: id, seed: Seed.instance.get() });
    }

    public createLAMPMapRequest(controlPointsId: RemoteObjectId, colNames: string[], controlPoints: PointSet, newColNames: string[]):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("lampMap",
            {controlPointsId: controlPointsId, colNames: colNames, newLowDimControlPoints: controlPoints, newColNames: newColNames});
    }
}

/**
 * This is an IDataView that is also a RemoteTableObject.
 * This is a base class for most views that are rendering information in a remote table.
 */
export abstract class RemoteTableObjectView extends RemoteTableObject implements IDataView {
    protected topLevel: HTMLElement;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId, protected page: FullPage) {
        super(remoteObjectId, originalTableId);
        this.setPage(page);
    }

    setPage(page: FullPage) {
        if (page == null)
            throw("null FullPage");
        this.page = page;
    }

    getPage() : FullPage {
        if (this.page == null)
            throw("Page not set");
        return this.page;
    }

    selectCurrent(): void {
        SelectedObject.instance.select(this, this.page.pageId);
    }

    public abstract refresh(): void;

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}

/**
 * A renderer that receives a remoteObjectId for a RemoteTableObject.
 */
export abstract class RemoteTableRenderer extends OnCompleteRenderer<RemoteObjectId> {
    protected remoteObject: RemoteTableObject;

    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string,
                       protected originalTableId: RemoteObjectId) { // may be null for the first table
        super(page, operation, description);
        this.remoteObject = null;
    }

    run(): void {
        if (this.value != null) {
            // If the originalTableId is null, this must be the first table we are receiving
            let originalTableId = this.originalTableId == null ? this.value : this.originalTableId;
            this.remoteObject = new RemoteTableObject(this.value, originalTableId);
        }
    }
}

/**
 * A zip receiver receives the result of a Zip operation on
 * two IDataSet<ITable> objects (an IDataSet<Pair<ITable, ITable>>,
 *  and applies to the pair the specified set operation setOp.
 */
export class ZipReceiver extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable,
                       protected setOp: CombineOperators,
                       protected originalTableId: RemoteObjectId,
                       // receiver constructs the renderer that is used to display
                       // the result after combining
                       protected receiver: (page: FullPage, operation: ICancellable) => RemoteTableRenderer) {
        super(page, operation, "zip", originalTableId);
    }

    run(): void {
        super.run();
        let rr = this.remoteObject.createSetOperationRequest(this.setOp);
        let rec = this.receiver(this.page, rr);
        rr.invoke(rec);
    }
}

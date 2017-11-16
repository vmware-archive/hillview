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

import {RpcRequest, RemoteObject, Renderer} from "./rpc";
import {EqualityFilterDescription} from "./dataViews/equalityFilter";
import {ICancellable, PartialResult, Seed} from "./util";
import {PointSet, Resolution} from "./ui/ui";
import {IDataView} from "./ui/dataview";
import {FullPage} from "./ui/fullPage";
import {
    CombineOperators,
    FilterDescription, Histogram3DArgs, HistogramArgs, IColumnDescription, RangeInfo,
    RecordOrder, Schema
} from "./javaBridge";
import {Histogram2DArgs} from "./javaBridge";
import {SelectedObject} from "./selectedObject";

/**
 * This class methods that correspond directly to TableTarget.java methods.
 */
export class RemoteTableObject extends RemoteObject {
    constructor(remoteObjectId: string) {
        super(remoteObjectId);
    }

    public createRangeRequest(r: RangeInfo): RpcRequest {
        return this.createRpcRequest("range", r);
    }

    public createZipRequest(r: RemoteObject): RpcRequest {
        return this.createRpcRequest("zip", r.remoteObjectId);
    }

    public createQuantileRequest(rowCount: number, o: RecordOrder, position: number): RpcRequest {
        return this.createRpcRequest("quantile", {
            precision: 100,
            tableSize: rowCount,
            order: o,
            position: position,
            seed: Seed.instance.get()
        });
    }

    public createNextKRequest(order: RecordOrder, firstRow: any[]): RpcRequest {
        let nextKArgs = {
            order: order,
            firstRow: firstRow,
            rowsOnScreen: Resolution.tableRowsOnScreen
        };
        return this.createRpcRequest("getNextK", nextKArgs);
    }

    public createGetSchemaRequest(): RpcRequest {
        return this.createRpcRequest("getSchema", null);
    }

    public createHLogLogRequest(colName: string) : RpcRequest {
    	return this.createRpcRequest("hLogLog", { columnName: colName, seed: Seed.instance.get() });
    }

    public createRange2DRequest(r1: RangeInfo, r2: RangeInfo): RpcRequest {
        return this.createRpcRequest("range2D", [r1, r2]);
    }

    public createRange2DColsRequest(c1: string, c2: string): RpcRequest {
        let r1: RangeInfo = new RangeInfo(c1, null);
        let r2: RangeInfo = new RangeInfo(c2, null);
        return this.createRange2DRequest(r1, r2);
    }

    public createHeavyHittersRequest(columns: IColumnDescription[],
                                     percent: number,
                                     totalRows: number,
                                     isMG: boolean): RpcRequest {
        if (isMG) {
            return this.createRpcRequest("heavyHittersMG",
                {columns: columns, amount: percent,
                    totalRows: totalRows, seed: Seed.instance.get() });
        } else {
            return this.createRpcRequest("heavyHitters",
                {columns: columns, amount: percent,
                    totalRows: totalRows, seed: Seed.instance.get() });

        }
    }

    public createCheckHeavyRequest(r: RemoteObject, schema: Schema): RpcRequest {
        return this.createRpcRequest("checkHeavy", {
            hittersId: r.remoteObjectId,
            schema: schema
        });
    }

    public createFilterHeavyRequest(r: RemoteObject, schema: Schema): RpcRequest {
        return this.createRpcRequest("filterHeavy", {
            hittersId: r.remoteObjectId,
            schema: schema
        });
    }

    public createProjectToEigenVectorsRequest(r: RemoteObject, dimension: number): RpcRequest {
        return this.createRpcRequest("projectToEigenVectors", {
            id: r.remoteObjectId,
            numComponents: dimension
        });
    }

    public createFilterEqualityRequest(filter: EqualityFilterDescription): RpcRequest {
        return this.createRpcRequest("filterEquality", filter);
    }

    public createCorrelationMatrixRequest(columnNames: string[]): RpcRequest {
        return this.createRpcRequest("correlationMatrix", {columnNames: columnNames});
    }

    public createFilterRequest(f: FilterDescription): RpcRequest {
        return this.createRpcRequest("filterRange", f);
    }

    public createFilter2DRequest(xRange: FilterDescription, yRange: FilterDescription): RpcRequest {
        return this.createRpcRequest("filter2DRange", {first: xRange, second: yRange});
    }

    public createHeatMapRequest(info: Histogram2DArgs): RpcRequest {
        return this.createRpcRequest("heatMap", info);
    }

    public createHeatMap3DRequest(info: Histogram3DArgs): RpcRequest {
        return this.createRpcRequest("heatMap3D", info);
    }

    public createHistogramRequest(info: HistogramArgs): RpcRequest {
        return this.createRpcRequest("histogram", info);
    }

    public createSetOperationRequest(setOp: CombineOperators): RpcRequest {
        return this.createRpcRequest("setOperation", CombineOperators[setOp]);
    }

    public createSampledControlPointsRequest(rowCount: number, numSamples: number, columnNames: string[]) {
        return this.createRpcRequest("sampledControlPoints",
            {rowCount: rowCount, numSamples: numSamples, columnNames: columnNames, seed: Seed.instance.get() });
    }

    public createCategoricalCentroidsControlPointsRequest(categoricalColumnName: string, numericalColumnNames: string[]) {
        return this.createRpcRequest("categoricalCentroidsControlPoints",
            {categoricalColumnName: categoricalColumnName, numericalColumnNames: numericalColumnNames});
    }

    public createMDSProjectionRequest(id: string) {
        return this.createRpcRequest("makeMDSProjection", { id: id, seed: Seed.instance.get() });
    }

    public createLAMPMapRequest(controlPointsId: string, colNames: string[], controlPoints: PointSet, newColNames: string[]) {
        return this.createRpcRequest("lampMap",
            {controlPointsId: controlPointsId, colNames: colNames, newLowDimControlPoints: controlPoints, newColNames: newColNames});
    }
}

/**
 * This is an IDataView that is also a RemoteTableObject.
 * This is a base class for most views that are rendering information in a remote table.
 */
export abstract class RemoteTableObjectView extends RemoteTableObject implements IDataView {
    protected topLevel: HTMLElement;

    constructor(remoteObjectId: string, protected page: FullPage) {
        super(remoteObjectId);
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
        SelectedObject.current.select(this, this.page.pageId);
    }

    public abstract refresh(): void;

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public scrollIntoView() {
        this.getHTMLRepresentation().scrollIntoView( { block: "end", behavior: "smooth" } );
    }
}

/**
 * A renderer that receives a remoteObjectId for a RemoteTableObject.
 */
export abstract class RemoteTableRenderer extends Renderer<string> {
    protected remoteObject: RemoteTableObject;

    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string) {
        super(page, operation, description);
        this.remoteObject = null;
    }

    public onNext(value: PartialResult<string>) {
        super.onNext(value);
        if (value.data != null) {
            if (this.remoteObject != null)
                throw "Remote object already set " + this.remoteObject;
            this.remoteObject = new RemoteTableObject(value.data);
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
                       // receiver constructs the renderer that is used to display
                       // the result after combining
                       protected receiver: (page: FullPage, operation: ICancellable) => RemoteTableRenderer) {
        super(page, operation, "zip");
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteObject == null)
            return;

        let rr = this.remoteObject.createSetOperationRequest(this.setOp);
        let rec = this.receiver(this.page, rr);
        rr.invoke(rec);
    }
}

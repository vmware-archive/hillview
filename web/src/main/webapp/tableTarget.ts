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

import {DatasetView, IViewSerialization} from "./datasetView";
import {
    CombineOperators, CreateColumnInfo, DataRange, FilterDescription,
    Heatmap, HistogramArgs,
    HistogramBase, HLogLog, IColumnDescription, kindIsString,
    NextKList, RangeArgs, RecordOrder, RemoteObjectId, Schema,
    TableSummary, TopList, NextKArgs, ComparisonFilterDescription,
    EigenVal, EqualityFilterDescription, FindResult, Heatmap3D,
} from "./javaBridge";
import {OnCompleteReceiver, RemoteObject, RpcRequest} from "./rpc";
import {FullPage} from "./ui/fullPage";
import {PointSet, ViewKind} from "./ui/ui";
import {ICancellable, Pair, PartialResult, Seed} from "./util";
import {IDataView} from "./ui/dataview";
import {SchemaClass} from "./schemaClass";

/**
 * This class has methods that correspond directly to TableTarget.java methods.
 */
export class TableTargetAPI extends RemoteObject {
    /**
     * Create a reference to a remote table target.
     * @param remoteObjectId   Id of remote table on the web server.
     */
    constructor(public readonly remoteObjectId: RemoteObjectId) {
        super(remoteObjectId);
    }

    public createZipRequest(r: RemoteObject): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("zip", r.remoteObjectId);
    }

    public createFindRequest(order: RecordOrder, topRow: any[], toFind: string, regex: boolean,
                             subString: boolean, caseSensitive: boolean):
        RpcRequest<PartialResult<FindResult>> {
        return this.createStreamingRpcRequest<FindResult>("find", {
            toFind: toFind,
            regex: regex,
            subString: subString,
            topRow: topRow,
            order: order,
            caseSensitive: caseSensitive
        });
    }

    public createQuantileRequest(rowCount: number, o: RecordOrder, position: number):
            RpcRequest<PartialResult<any[]>> {
        return this.createStreamingRpcRequest<any[]>("quantile", {
            precision: 100,
            tableSize: rowCount,
            order: o,
            position: position,
            seed: Seed.instance.get(),
        });
    }

    public createDataRangeRequest(cd: IColumnDescription, cdfBuckets: number):
        RpcRequest<PartialResult<DataRange>> {
        const seed = kindIsString(cd.kind) ? Seed.instance.get() : 0;
        const args: RangeArgs = {
            cd: cd,
            seed: seed,
            stringsToSample: cdfBuckets };
        return this.createStreamingRpcRequest<DataRange>(
            "getDataRange", args);
    }

    public createDataRangesRequest(cds: IColumnDescription[], buckets: number[]):
        RpcRequest<PartialResult<DataRange[]>> {
        console.assert(cds.length === buckets.length);
        const args: RangeArgs[] = [];
        for (let i = 0; i < cds.length; i++) {
            const cd = cds[i];
            const seed = kindIsString(cd.kind) ? Seed.instance.get() : 0;
            const arg: RangeArgs = {
                cd: cd,
                seed: seed,
                stringsToSample: buckets[i]
            };
            args.push(arg);
        }
        const method = cds.length === 3 ? "getDataRanges3D" : "getDataRanges2D";
        return this.createStreamingRpcRequest<DataRange>(method, args);
    }

    public createNextKRequest(order: RecordOrder, firstRow: any[] | null, rowCount: number):
        RpcRequest<PartialResult<NextKList>> {
        const nextKArgs: NextKArgs = {
            toFind: null,
            order: order,
            firstRow: firstRow,
            rowsOnScreen: rowCount,
        };
        return this.createStreamingRpcRequest<NextKList>("getNextK", nextKArgs);
    }

    public createGetSchemaRequest(): RpcRequest<PartialResult<TableSummary>> {
        return this.createStreamingRpcRequest<TableSummary>("getSchema", null);
    }

    public createHLogLogRequest(colName: string): RpcRequest<PartialResult<HLogLog>> {
        return this.createStreamingRpcRequest<HLogLog>("hLogLog",
            { columnName: colName, seed: Seed.instance.get() });
    }

    public createHeavyHittersRequest(columns: IColumnDescription[],
                                     percent: number,
                                     totalRows: number,
                                     threshold: number): RpcRequest<PartialResult<TopList>> {
        if (percent < threshold) {
            return this.createStreamingRpcRequest<TopList>("heavyHittersMG",
                {columns: columns, amount: percent,
                    totalRows: totalRows, seed: Seed.instance.get() });
        } else {
            return this.createStreamingRpcRequest<TopList>("heavyHittersSampling",
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

    public createFilterHeavyRequest(rid: RemoteObjectId, schema: Schema, includeSet: boolean):
        RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterHeavy", {
            hittersId: rid,
            schema: schema,
            includeSet: includeSet
        });
    }

    public createFilterListHeavy(rid: RemoteObjectId, schema: Schema, includeSet: boolean, rowIndices: number[]):
        RpcRequest<PartialResult<RemoteObjectId>> {
            return this.createStreamingRpcRequest<RemoteObjectId>("filterListHeavy", {
                hittersId: rid,
                schema: schema,
                includeSet: includeSet,
                rowIndices: rowIndices
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

    public createFilterComparisonRequest(filter: ComparisonFilterDescription):
    RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterComparison", filter);
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

    public createSpectrumRequest(columnNames: string[], totalRows: number, toSample: boolean):
    RpcRequest<PartialResult<EigenVal>> {
        return this.createStreamingRpcRequest<EigenVal>("spectrum", {
            columnNames: columnNames,
            totalRows: totalRows,
            seed: Seed.instance.get(),
            toSample: toSample
        });
    }

    public createCreateColumnRequest(c: CreateColumnInfo):
        RpcRequest<PartialResult<string>> {
        return this.createStreamingRpcRequest<string>("createColumn", c);
    }

    public createFilterRequest(f: FilterDescription):
        RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterRange", f);
    }

    public createFilter2DRequest(xRange: FilterDescription, yRange: FilterDescription):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filter2DRange",
            {first: xRange, second: yRange});
    }

    public createHistogram2DRequest(info: HistogramArgs[]):
        RpcRequest<PartialResult<Pair<Heatmap, HistogramBase>>> {
        return this.createStreamingRpcRequest<Pair<Heatmap, HistogramBase>>("histogram2D", info);
    }

    public createHeatmapRequest(info: HistogramArgs[]): RpcRequest<PartialResult<Heatmap>> {
        return this.createStreamingRpcRequest<Heatmap>("heatmap", info);
    }

    public createHeatmap3DRequest(info: HistogramArgs[]): RpcRequest<PartialResult<Heatmap3D>> {
        return this.createStreamingRpcRequest<Heatmap3D>("heatmap3D", info);
    }

    public createHistogramRequest(info: HistogramArgs):
        RpcRequest<PartialResult<HistogramBase>> {
        return this.createStreamingRpcRequest<HistogramBase>(
            "histogram", info);
    }

    public createSetOperationRequest(setOp: CombineOperators): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("setOperation", CombineOperators[setOp]);
    }

    public createSampledControlPointsRequest(rowCount: number, numSamples: number, columnNames: string[]):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("sampledControlPoints",
            {rowCount: rowCount, numSamples: numSamples, columnNames: columnNames, seed: Seed.instance.get() });
    }

    public createCategoricalCentroidsControlPointsRequest(
        categoricalColumnName: string, numericalColumnNames: string[]):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("categoricalCentroidsControlPoints", {
                categoricalColumnName: categoricalColumnName,
                numericalColumnNames: numericalColumnNames} );
    }

    public createMDSProjectionRequest(id: RemoteObjectId): RpcRequest<PartialResult<PointSet>> {
        return this.createStreamingRpcRequest<PointSet>(
            "makeMDSProjection", { id: id, seed: Seed.instance.get() });
    }

    public createLAMPMapRequest(controlPointsId: RemoteObjectId,
                                colNames: string[], controlPoints: PointSet, newColNames: string[]):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("lampMap",
            {controlPointsId: controlPointsId, colNames: colNames,
                newLowDimControlPoints: controlPoints, newColNames: newColNames});
    }
}

/**
 * This is an IDataView that is also a TableTargetAPI.
 * "Big" tables are table-shaped remote datasets, represented
 * in Java by IDataSet<ITable>.
 * This is a base class for most views that are rendering
 * information from a big table.
 * A BigTableView view is always part of a DatasetView.
 */
export abstract class BigTableView extends TableTargetAPI implements IDataView {
    protected topLevel: HTMLElement;
    public readonly dataset: DatasetView;

    /**
     * Create a view for a big table.
     * @param remoteObjectId   Id of remote table on the web server.
     * @param schema           Schema of the current view (usually a subset of the schema of the
     *                         big table).
     * @param rowCount         Total number of rows in the big table.
     * @param page             Page where the view is displayed.
     * @param viewKind         Kind of view displayed.
     */
    protected constructor(
        remoteObjectId: RemoteObjectId,
        public rowCount: number,
        public schema: SchemaClass,
        public page: FullPage,
        public readonly viewKind: ViewKind) {
        super(remoteObjectId);
        this.setPage(page);
        this.dataset = page.dataset;
    }

    /**
     * Save the information needed to (re)create this view.
     */
    public serialize(): IViewSerialization {
        return {
            viewKind: this.viewKind,
            pageId: this.page.pageId,
            sourcePageId: this.page.sourcePageId,
            title: this.page.title,
            remoteObjectId: this.remoteObjectId,
            rowCount: this.rowCount,
            schema: this.schema.serialize(),
        };
    }

    public setPage(page: FullPage): void {
        if (page == null)
            throw new Error(("null FullPage"));
        this.page = page;
        if (this.topLevel != null) {
            this.topLevel.ondragover = (e) => e.preventDefault();
            this.topLevel.ondrop = (e) => this.drop(e);
        }
    }

    // noinspection JSMethodCanBeStatic
    public drop(e: DragEvent): void { console.log(e); }

    public getPage(): FullPage {
        if (this.page == null)
            throw new Error(("Page not set"));
        return this.page;
    }

    public selectCurrent(): void {
        this.dataset.select(this, this.page.pageId);
    }

    public abstract resize(): void;
    public abstract refresh(): void;

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public abstract combine(op: CombineOperators): void;
}

/**
 * A renderer that receives a remoteObjectId for a big table.
 */
export abstract class BaseRenderer extends OnCompleteReceiver<RemoteObjectId> {
    protected remoteObject: TableTargetAPI;

    protected constructor(public page: FullPage,
                          public operation: ICancellable<RemoteObjectId>,
                          public description: string,
                          protected dataset: DatasetView) { // may be null for the first table
        super(page, operation, description);
        this.remoteObject = null;
    }

    public run(): void {
        if (this.value != null)
            this.remoteObject = new TableTargetAPI(this.value);
    }
}

/**
 * A zip receiver receives the result of a Zip operation on
 * two IDataSet<ITable> objects (an IDataSet<Pair<ITable, ITable>>,
 *  and applies to the pair the specified set operation setOp.
 */
export class ZipReceiver extends BaseRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       protected setOp: CombineOperators,
                       protected dataset: DatasetView,
                       // receiver constructs the renderer that is used to display
                       // the result after combining
                       protected receiver:
                           (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseRenderer) {
        super(page, operation, "zip", dataset);
    }

    public run(): void {
        super.run();
        const rr = this.remoteObject.createSetOperationRequest(this.setOp);
        const rec = this.receiver(this.page, rr);
        rr.invoke(rec);
    }
}

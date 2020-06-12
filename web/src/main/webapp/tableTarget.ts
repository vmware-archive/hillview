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
 * This file contains lots of methods for accessing the remote TableTarget.java class.
 */

import {DatasetView, IViewSerialization} from "./datasetView";
import {
    AggregateDescription,
    BasicColStats,
    BucketsInfo,
    CombineOperators,
    ComparisonFilterDescription,
    ContainsArgs,
    CountWithConfidence,
    EigenVal,
    FilterDescription,
    FindResult, Groups,
    HeavyHittersFilterInfo,
    HistogramRequestInfo,
    IColumnDescription,
    JSCreateColumnInfo,
    JSFilterInfo,
    kindIsString,
    KVCreateColumnInfo,
    NextKArgs,
    NextKList, QuantilesMatrixInfo,
    QuantilesVectorInfo,
    RangeArgs,
    RecordOrder,
    RemoteObjectId,
    RowFilterDescription, SampleSet,
    Schema,
    StringColumnFilterDescription,
    StringColumnsFilterDescription,
    StringFilterDescription,
    TableSummary,
    TopList
} from "./javaBridge";
import {OnCompleteReceiver, RemoteObject, RpcRequest} from "./rpc";
import {FullPage, PageTitle} from "./ui/fullPage";
import {PointSet, Resolution, ViewKind} from "./ui/ui";
import {assert, ICancellable, Pair, PartialResult, Seed, Two} from "./util";
import {IDataView} from "./ui/dataview";
import {SchemaClass} from "./schemaClass";
import {PlottingSurface} from "./ui/plottingSurface";

/**
 * An interface which has a function that is called when all updates are completed.
 */
export interface CompletedWithTime {
    updateCompleted(timeInMs: number): void;
}

export interface OnNextK extends CompletedWithTime {
    updateView(nextKList: NextKList,
               revert: boolean,
               order: RecordOrder,
               result: FindResult): void;
}

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

    public createSetRequest(r: RemoteObject, c: CombineOperators): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("setOperation",
            { otherId: r.remoteObjectId, op: CombineOperators[c] });
    }

    public createFindRequest(
        order: RecordOrder, topRow: any[],
        strFilter: StringFilterDescription, excludeTopRow: boolean, next: boolean):
        RpcRequest<PartialResult<FindResult>> {
        return this.createStreamingRpcRequest<FindResult>("find", {
            order: order,
            topRow: topRow,
            stringFilterDescription: strFilter,
            excludeTopRow: excludeTopRow,
            next: next,
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

    /**
     * Computes the maximum resolution at which a data range request must be made.
     * @param page      Page - used to compute the screen size.
     * @param viewKind  Desired view for the data.
     */
    private static rangesResolution(page: FullPage, viewKind: ViewKind): number[] {
        const width = page.getWidthInPixels();
        const size = PlottingSurface.getDefaultCanvasSize(width);
        const maxWindows = Math.floor(width / Resolution.minTrellisWindowSize) *
            Math.floor(size.height / Resolution.minTrellisWindowSize);
        switch (viewKind) {
            case "QuartileVector":
                return [Resolution.maxBucketCount, Resolution.maxBucketCount];
            case "Histogram":
                // Always get the window size; we integrate the CDF to draw the actual histogram.
                return [size.width];
            case "2DHistogram":
                // On the horizontal axis we get the maximum resolution, which we will use for
                // deriving the CDF curve.  On the vertical axis we use a smaller number.
                return [width, Resolution.maxBucketCount];
            case "Heatmap":
                return [Math.floor(size.width / Resolution.minDotSize),
                        Math.floor(size.height / Resolution.minDotSize)];
            case "Trellis2DHistogram":
            case "TrellisHeatmap":
                return [width, Resolution.maxBucketCount, maxWindows];
            case "TrellisQuartiles":
                return [Resolution.maxBucketCount, Resolution.maxBucketCount, maxWindows];
            case "TrellisHistogram":
                return [width, maxWindows];
            default:
                assert(false, "Unhandled case " + viewKind);
                return null;
        }
    }

    /**
     * Create a request to find quantiles of a set of columns for a specific screen resolution.
     * @param cds        Columns whose quantiles are computed.
     * @param page       Current page.
     * @param viewKind   How data will be displayed.
     */
    public createDataQuantilesRequest(cds: IColumnDescription[], page: FullPage, viewKind: ViewKind):
        RpcRequest<PartialResult<BucketsInfo[]>> {
        // Determine the resolution of the ranges request based on the plot kind.
        const bucketCounts: number[] = TableTargetAPI.rangesResolution(page, viewKind);
        assert(bucketCounts.length === cds.length);
        const args: RangeArgs[] = [];
        for (let i = 0; i < cds.length; i++) {
            const cd = cds[i];
            const seed = kindIsString(cd.kind) ? Seed.instance.get() : 0;
            const arg: RangeArgs = {
                cd: cd,
                seed: seed,
                stringsToSample: bucketCounts[i]
            };
            args.push(arg);
        }
        return this.createStreamingRpcRequest<BucketsInfo>("getDataQuantiles", args);
    }

    public createQuantilesVectorRequest(args: QuantilesVectorInfo):
        RpcRequest<PartialResult<Groups<SampleSet>>> {
        return this.createStreamingRpcRequest<Groups<SampleSet>>("getQuantilesVector", args);
    }

    public createQuantilesMatrixRequest(args: QuantilesMatrixInfo):
        RpcRequest<PartialResult<Groups<Groups<SampleSet>>>> {
        return this.createStreamingRpcRequest<Groups<Groups<SampleSet>>>("getQuantilesMatrix", args);
    }

    public createContainsRequest(order: RecordOrder, row: any[]): RpcRequest<RemoteObjectId> {
        const args: ContainsArgs = {
            order: order,
            row: row
        };
        return this.createStreamingRpcRequest<RemoteObjectId>("contains", args);
    }

    public createGetLogFragmentRequest(schema: Schema, row: any[], rowSchema: Schema, rowCount: number):
        RpcRequest<PartialResult<NextKList>> {
        return this.createStreamingRpcRequest<NextKList>("getLogFragment", {
            schema: schema,
            row: row,
            rowSchema: rowSchema,
            count: rowCount
        });
    }

    /**
     * Create a request for a nextK sketch
     * @param order            Sorting order.
     * @param firstRow         Values in the smallest row (may be null).
     * @param rowsOnScreen     How many rows to bring.
     * @param aggregates       List of aggregates to compute.
     * @param columnsNoValue   List of columns in the firstRow for which we want to specify
     *                         "minimum possible value" instead of "null".
     */
    public createNextKRequest(order: RecordOrder, firstRow: any[] | null, rowsOnScreen: number,
                              aggregates?: AggregateDescription[], columnsNoValue?: string[]):
        RpcRequest<PartialResult<NextKList>> {
        const nextKArgs: NextKArgs = {
            toFind: null,
            order: order,
            firstRow: firstRow,
            rowsOnScreen: rowsOnScreen,
            columnsNoValue: columnsNoValue,
            aggregates
        };
        return this.createStreamingRpcRequest<NextKList>("getNextK", nextKArgs);
    }

    public createGetSummaryRequest(): RpcRequest<PartialResult<TableSummary>> {
        return this.createStreamingRpcRequest<TableSummary>("getSummary", null);
    }

    public createHLogLogRequest(colName: string): RpcRequest<PartialResult<CountWithConfidence>> {
        return this.createStreamingRpcRequest<CountWithConfidence>("hLogLog",
            { columnName: colName, seed: Seed.instance.get() });
    }

    public createBasicColStatsRequest(cols: string[]): RpcRequest<PartialResult<BasicColStats[]>> {
        return this.createStreamingRpcRequest<BasicColStats[]>("basicColStats", cols);
    }

    public createHeavyHittersRequest(columns: IColumnDescription[],
                                     percent: number,
                                     totalRows: number,
                                     threshold: number): RpcRequest<PartialResult<TopList>> {
        if (percent < threshold) {
            return this.createStreamingRpcRequest<TopList>("heavyHittersMG",
                { columns: columns, amount: percent,
                    totalRows: totalRows, seed: 0 });  // no randomness needed
        } else {
            return this.createStreamingRpcRequest<TopList>("heavyHittersSampling",
                { columns: columns, amount: percent,
                    totalRows: totalRows, seed: Seed.instance.get() });
        }
    }

    public createCheckHeavyRequest(r: RemoteObject, schema: Schema):
            RpcRequest<PartialResult<TopList>> {
        return this.createStreamingRpcRequest<TopList>("checkHeavy", {
            hittersId: r.remoteObjectId,
            schema: schema
        } as HeavyHittersFilterInfo);
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

    public createJSFilterRequest(filter: JSFilterInfo): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("jsFilter", filter);
    }

    public createRowFilterRequest(filter: RowFilterDescription):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterOnRow", filter);
    }

    public createFilterColumnRequest(filter: StringColumnFilterDescription):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterColumn", filter);
    }

    public createFilterColumnsRequest(filter: StringColumnsFilterDescription):
        RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterColumns", filter);
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

    public createProjectRequest(schema: Schema): RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("project", schema);
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

    public createJSCreateColumnRequest(c: JSCreateColumnInfo):
        RpcRequest<PartialResult<string>> {
        return this.createStreamingRpcRequest<string>("jsCreateColumn", c);
    }

    public createKVCreateColumnRequest(c: KVCreateColumnInfo):
        RpcRequest<PartialResult<string>> {
        return this.createStreamingRpcRequest<string>("kvCreateColumn", c);
    }

    public createFilterRequest(f: FilterDescription):
        RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filterRange", f);
    }

    public createFilter2DRequest(xRange: FilterDescription, yRange: FilterDescription):
            RpcRequest<PartialResult<RemoteObjectId>> {
        return this.createStreamingRpcRequest<RemoteObjectId>("filter2DRange", { first: xRange, second: yRange } );
    }

    public createHistogram2DAndCDFRequest(info: HistogramRequestInfo[]):
        RpcRequest<PartialResult<Pair<Groups<Groups<number>>, Groups<number>>>> {
        return this.createStreamingRpcRequest<Pair<Groups<Groups<number>>, Groups<number>>>("histogram2DAndCDF", info);
    }

    public createHistogram2DRequest(info: HistogramRequestInfo[]): RpcRequest<PartialResult<Two<Groups<Groups<number>>>>> {
        return this.createStreamingRpcRequest<Two<Groups<Groups<number>>>>("histogram2D", info);
    }

    public createHistogram3DRequest(info: HistogramRequestInfo[]): RpcRequest<PartialResult<Groups<Groups<Groups<number>>>>> {
        return this.createStreamingRpcRequest<Groups<Groups<Groups<number>>>>("histogram3D", info);
    }

    public createHistogramAndCDFRequest(info: HistogramRequestInfo[]):
    RpcRequest<PartialResult<Pair<Groups<number>, Groups<number>>>> {
        return this.createStreamingRpcRequest<Pair<Groups<number>, Groups<number>>>(
            "histogramAndCDF", info);
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
                numericalColumnNames: numericalColumnNames } );
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
                newLowDimControlPoints: controlPoints, newColNames: newColNames });
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
export abstract class BigTableView extends TableTargetAPI implements IDataView, CompletedWithTime {
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
            title: this.page.title.format,
            provenance: this.page.title.provenance,
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
        this.dataset.select(this.page.pageId);
    }

    public abstract resize(): void;
    public abstract refresh(): void;

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    /**
     * This method is called by the zip receiver after combining two datasets.
     * It should return a renderer which will handle the newly received object
     * after the set operation has been performed.
     */
    protected abstract getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver;

    public combine(how: CombineOperators): void {
        const pageId = this.dataset.getSelected();
        if (pageId == null) {
            this.page.reportError("No original dataset selected");
            return;
        }

        const view = this.dataset.findPage(pageId).dataView;
        const rr = this.createSetRequest(view as BigTableView, how);
        const renderer = this.getCombineRenderer(
            new PageTitle(this.page.title.format,
                CombineOperators[how] + " between " + this.page.pageId + " and " + pageId));
        const receiver = renderer(this.getPage(), rr);
        rr.invoke(receiver);
    }

    /**
     * This method is called when all the data has been received.
     */
    public updateCompleted(timeInMs: number): void {
        this.page.reportTime(timeInMs);
    }

    public isPrivate(): boolean {
        return this.dataset.isPrivate();
    }
}

/**
 * A receiver that receives a remoteObjectId for a big table.
 */
export abstract class BaseReceiver extends OnCompleteReceiver<RemoteObjectId> {
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

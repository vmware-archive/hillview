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

import {Receiver} from "../rpc";
import {
    BucketsInfo,
    Groups,
    HistogramRequestInfo,
    IColumnDescription, RangeFilterArrayDescription,
    RecordOrder,
    RemoteObjectId, SampleSet
} from "../javaBridge";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {SchemaClass} from "../schemaClass";
import {
    allBuckets,
    Converters, describeQuartiles,
    ICancellable,
    PartialResult, quartileAsCsv, saveAs,
} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {
    IViewSerialization,
    TrellisQuartilesSerialization
} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {ChartOptions, Resolution} from "../ui/ui";
import {SubMenu, TopMenu} from "../ui/menu";
import {
    FilterReceiver,
    DataRangesReceiver,
    TrellisShape, TrellisLayoutComputation,
} from "./dataRangesReceiver";
import {TrellisChartView} from "./trellisChartView";
import {NextKReceiver, TableView} from "./tableView";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {Quartiles2DPlot} from "../ui/quartiles2DPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {BucketDialog} from "./histogramViewBase";

export class TrellisHistogramQuartilesView extends TrellisChartView<Groups<Groups<SampleSet>>> {
    protected hps: Quartiles2DPlot[];
    protected xAxisData: AxisData;
    private readonly defaultProvenance: string = "Trellis Quartiles";

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected qCol: IColumnDescription,
        protected shape: TrellisShape,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "TrellisQuartiles");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.hps = [];
        this.data = null;

        this.menu = new TopMenu( [this.exportMenu(),
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                }, { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying view using a table view.",
                }, { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw the histograms. "
                }, { text: "heatmap",
                    action: () => this.heatmap(),
                    help: "Show this data as a Trellis plot of heatmaps.",
                }, { text: "# groups...",
                    action: () => this.changeGroups(),
                    help: "Change the number of groups."
                }
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.hps = [];
        this.createAllSurfaces( (surface) => {
            const hp = new Quartiles2DPlot(surface);
            this.hps.push(hp);
        });
    }

    public setAxes(xAxisData: AxisData,
                   groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.groupByAxisData = groupByAxisData;
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
                return null;
            case "GAxis":
                return this.groupByAxisData;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                // TODO
                return null;
        }
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.data == null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        let ranges = [];
        if (eventKind === "XAxis") {
            ranges = [sourceRange, this.groupByAxisData.dataRange];
        } else if (eventKind === "YAxis") {
            // TODO
            return;
            /*
            this.relative = false;
            this.updateView(this.data, [0, 0, 0]);
            return;
             */
        } else if (eventKind === "GAxis") {
            ranges = [this.xAxisData.dataRange, sourceRange];
        } else {
            return;
        }

        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema, [0, 0, 0],  // any number of buckets
            [this.xAxisData.description, this.qCol, this.groupByAxisData.description],
            this.page.title, Converters.eventToString(pageId, eventKind), {
                chartKind: "TrellisQuartiles", 
                reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    protected onMouseMove(): void {
        const mousePosition = this.checkMouseBounds();
        if (mousePosition == null)
            return;

        const plot = this.hps[mousePosition.plotIndex];
        if (plot == null)
            return;

        this.pointDescription.show(true);
        let xs = this.xAxisData.invert(mousePosition.x);

        // Use the plot scale, not the yData to invert.  That's the
        // one which is used to draw the axis.
        let bucketDesc = "";
        let min = "", q1 = "", q2 = "", q3 = "", max = "", missing = "", count = "", group = "";
        if (this.xAxisData.scale != null) {
            xs = this.xAxisData.invert(mousePosition.x);
            if (this.data != null) {
                const bucket = plot.getBucketIndex(mousePosition.x);
                if (bucket < 0)
                    return;
                bucketDesc = this.xAxisData.bucketDescription(bucket, 20);
                const qv = this.data.perBucket[mousePosition.plotIndex].perBucket[bucket];
                group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex, 40);
                [count, missing, min, q1, q2, q3, max] = describeQuartiles(qv);
            }
        }
        const position = d3mouse(this.surface.getCanvas().node());
        this.pointDescription.update([xs, group, bucketDesc, max, q3, q2, q1, min, count, missing],
            position[0], position[1]);
    }

    protected showTable(): void {
        const newPage = this.dataset.newPage(new PageTitle("Table", this.defaultProvenance), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.qCol,
            isAscending: true
        }, {
            columnDescription: this.groupByAxisData.description,
            isAscending: true
        }]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
    }

    protected chooseBuckets(): void {
        const bucketDialog = new BucketDialog(
            this.xAxisData.bucketCount, Resolution.maxBuckets(this.page.getWidthInPixels()));
        bucketDialog.setAction(() => {
            const bucketCount = bucketDialog.getBucketCount();
            if (bucketCount == null)
                return;
            const cds = [this.xAxisData.description, this.qCol, this.groupByAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisQuartiles");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [bucketCount, 0, this.groupByAxisData.bucketCount],
                cds, null, "changed buckets",{
                    reusePage: true, chartKind: "TrellisQuartiles"
                }));
        });
        bucketDialog.show();
    }

    protected doChangeGroups(groupCount: number): void {
        if (groupCount == null) {
            this.page.reportError("Illegal group count");
            return;
        }
        if (groupCount === 1) {
            const cds = [this.xAxisData.description,
                this.qCol];
            const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [0, 0],
                cds, null, "change groups",{
                    reusePage: true, chartKind: "QuartileVector"
                }));
        } else {
            const cds = [this.xAxisData.description,
                this.qCol, this.groupByAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisQuartiles");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [0, 0, groupCount],
                cds, null, "change groups",{
                    reusePage: true, 
                    chartKind: "TrellisQuartiles"
                }));
        }
    }

    protected heatmap(): void {
        const cds = [this.xAxisData.description,
            this.qCol, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0, this.shape.windowCount],
            cds, null, this.defaultProvenance,{
                reusePage: false, chartKind: "TrellisHeatmap", exact: true
            }));
    }

    protected export(): void {
        let lines = [];
        const axisName = this.schema.displayName(this.groupByAxisData.description.name);
        let bucketNo = 0;
        for (const g of allBuckets(this.data)) {
            const desc = this.groupByAxisData.bucketDescription(bucketNo++, 0);
            const l = quartileAsCsv(g, this.schema, this.xAxisData);
            lines = lines.concat(l.map(line => axisName + " " + desc + "," + line));
        }

        const fileName = "quantiles3d.csv";
        saveAs(fileName, lines.join("\n"));
    }

    public resize(): void {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        this.shape = TrellisLayoutComputation.resize(chartSize.width, chartSize.height, this.shape);
        this.updateView(this.data, null);
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.qCol, this.groupByAxisData.description];
        const ranges = [this.xAxisData.dataRange, null, this.groupByAxisData.dataRange];
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema,
            [this.xAxisData.bucketCount, this.groupByAxisData.bucketCount],
            cds, this.page.title, null,{
                chartKind: "TrellisQuartiles", reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const ser: TrellisQuartilesSerialization = {
            ...super.serialize(),
            ...this.shape,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.qCol,
            xBucketCount: this.xAxisData.bucketCount,
            groupByColumn: this.groupByAxisData.description
        };
        return ser;
    }

    public static reconstruct(ser: TrellisQuartilesSerialization, page: FullPage): IDataView {
        if (ser.remoteObjectId == null || ser.rowCount == null || ser.xWindows == null ||
            ser.yWindows == null || ser.windowCount ||
            ser.schema == null)
            return null;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const shape = TrellisChartView.deserializeShape(ser, page);
        const view = new TrellisHistogramQuartilesView(ser.remoteObjectId, ser.rowCount,
            schema, ser.columnDescription1, shape, page);
        view.setAxes(new AxisData(ser.columnDescription0, null, ser.xBucketCount),
            new AxisData(ser.groupByColumn, null, ser.windowCount));
        return view;
    }

    public updateView(data: Groups<Groups<SampleSet>>, yAxisRange: [number, number] | null): void {
        if (data == null || data.perBucket == null)
            return;
        this.createNewSurfaces();
        this.data = data;
        let min = null, max = null;

        if (yAxisRange == null) {
            for (const buckets of allBuckets(data)) {
                for (const bucket of allBuckets(buckets)) {
                    if (bucket.count === 0)
                        continue;
                    if (min == null) {
                        min = bucket.min;
                        max = bucket.max;
                    } else {
                        min = Math.min(min, bucket.min);
                        max = Math.max(max, bucket.max);
                    }
                }
            }
            yAxisRange = [min, max];
        }

        for (let i = 0; i < data.perBucket.length; i++) {
            const buckets = data.perBucket[i];
            const plot = this.hps[i];
            plot.setData(buckets, this.schema, this.rowCount, this.xAxisData, this.isPrivate(), yAxisRange);
            plot.draw();
        }
        if (this.shape.missingBucket) {
            const buckets = data.perMissing;
            const plot = this.hps[data.perBucket.length];
            plot.setData(buckets, this.schema, this.rowCount, this.xAxisData, this.isPrivate(), yAxisRange);
            plot.draw();
        }

        // We draw the axes after drawing the data
        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom, PlottingSurface.bottomMargin);
        const yAxis = this.hps[0].getYAxis();
        this.drawAxes(this.xAxisData.axis, yAxis);
        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getDisplayNameString(this.schema),
                this.groupByAxisData.getDisplayNameString(this.schema),
                "bucket", "max", "q3", "median", "q1", "min", "count", "missing"], 40);
        this.pointDescription.show(false);
    }

    protected dragMove(): boolean {
        if (!super.dragMove())
            return false;
        const index = this.selectionIsLocal();
        if (index != null) {
            // Adjust the selection rectangle size to cover the whole vertical space
            this.selectionRectangle
                .attr("height", this.shape.size.height)
                .attr("y", this.coordinates[index].y);
        }
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(title, [this.xAxisData.description, this.qCol,
                this.groupByAxisData.description], this.schema, [0, 0, 0], page, operation, this.dataset, {
                chartKind: "TrellisQuartiles", reusePage: false,
            });
        };
    }

    protected filter(filter: RangeFilterArrayDescription): void {
        if (filter == null)
            return;
        const rr = this.createFilterRequest(filter);
        const title = new PageTitle(this.page.title.format, Converters.filterArrayDescription(filter));
        const renderer = new FilterReceiver(title, [this.xAxisData.description, this.qCol,
            this.groupByAxisData.description], this.schema, [0, 0, 0], this.page, rr, this.dataset, {
            chartKind: "TrellisQuartiles",
            reusePage: false,
        });
        rr.invoke(renderer);
    }

    protected selectionCompleted(): void {
        const local = this.selectionIsLocal();
        if (local != null) {
            const origin = this.canvasToChart(this.selectionOrigin);
            const left = this.position(origin.x, origin.y);
            const end = this.canvasToChart(this.selectionEnd);
            const right = this.position(end.x, end.y);
            const filter = this.xAxisData.getFilter(left.x, right.x);
            const fa = { filters: [filter], complement: d3event.sourceEvent.ctrlKey };
            this.filter(fa);
        } else {
            const filter = this.getGroupBySelectionFilter();
            this.filter(filter);
        }
    }
}

/**
 * Renders a Trellis plot of quartile vectors
 */
export class TrellisHistogramQuartilesReceiver extends Receiver<Groups<Groups<SampleSet>>> {
    protected trellisView: TrellisHistogramQuartilesView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected histoArgs: HistogramRequestInfo[],
                protected range: BucketsInfo[],
                protected qCol: IColumnDescription,
                protected shape: TrellisShape,
                operation: ICancellable<Groups<Groups<SampleSet>>>,
                protected options: ChartOptions) {
        super(options.reusePage ? page : page.dataset.newPage(title, page), operation, "quartiles");
        this.trellisView = new TrellisHistogramQuartilesView(
            remoteTable.remoteObjectId, rowCount, schema, qCol,
            this.shape, this.page);
        const xAxis = new AxisData(histoArgs[0].cd, range[0], histoArgs[0].bucketCount);
        const groupByAxis = new AxisData(histoArgs[1].cd, range[1], histoArgs[1].bucketCount);
        this.trellisView.setAxes(xAxis, groupByAxis);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Groups<Groups<SampleSet>>>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.trellisView.updateView(value.data, null);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.trellisView.updateCompleted(this.elapsedMilliseconds());
    }
}

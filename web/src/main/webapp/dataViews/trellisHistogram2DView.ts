/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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
    FilterDescription, Groups,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {SchemaClass} from "../schemaClass";
import {add, Converters, ICancellable, PartialResult, percent, reorder, significantDigits} from "../util";
import {AxisData, AxisKind} from "./axisData";
import {
    IViewSerialization,
    TrellisHistogram2DSerialization
} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {ChartOptions, Resolution} from "../ui/ui";
import {SubMenu, TopMenu} from "../ui/menu";
import {Histogram2DPlot} from "../ui/histogram2DPlot";
import {
    FilterReceiver,
    DataRangesReceiver,
    TrellisShape,
    TrellisLayoutComputation
} from "./dataRangesCollectors";
import {TrellisChartView} from "./trellisChartView";
import {NextKReceiver, TableView} from "./tableView";
import {BucketDialog} from "./histogramViewBase";
import {TextOverlay} from "../ui/textOverlay";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {HistogramLegendPlot} from "../ui/histogramLegendPlot";
import {event as d3event, mouse as d3mouse} from "d3-selection";

export class TrellisHistogram2DView extends TrellisChartView {
    protected hps: Histogram2DPlot[];
    protected buckets: number;
    protected xAxisData: AxisData;
    protected legendAxisData: AxisData;
    private legendSurface: HtmlPlottingSurface;
    private readonly legendDiv: HTMLDivElement;
    protected legendPlot: HistogramLegendPlot;
    protected relative: boolean;
    protected data: Groups<Groups<Groups<number>>>;
    protected maxYAxis: number | null;  // maximum value to use for Y axis; if null - derive from data
    private readonly defaultProvenance: string = "Trellis 2D histograms";

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected shape: TrellisShape,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "Trellis2DHistogram");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.hps = [];
        this.data = null;
        this.maxYAxis = null;

        this.menu = new TopMenu( [{
            text: "Export",
            help: "Save the information in this view in a local file.",
            subMenu: new SubMenu([{
                text: "As CSV",
                help: "Saves the data in this view in a CSV file.",
                action: () => { this.export(); },
            }]),
        }, { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                }, { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying view using a table view.",
                }, { text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this data without making any approximations.",
                }, { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw the histograms. " +
                        "The number of buckets must be between 1 and " + Resolution.maxBucketCount,
                }, { text: "swap axes",
                    action: () => this.swapAxes(),
                    help: "Swap the X and Y axes of all plots."
                }, { text: "heatmap",
                    action: () => this.heatmap(),
                    help: "Show this data as a Trellis plot of heatmaps.",
                }, { text: "# groups...",
                    action: () => this.changeGroups(),
                    help: "Change the number of groups."
                }, {
                    text: "relative/absolute",
                    action: () => this.toggleNormalize(),
                    help: "In an absolute plot the Y axis represents the size for a bucket. " +
                        "In a relative plot all bars are normalized to 100% on the Y axis.",
                }
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.buckets = Math.round(shape.size.width / Resolution.minBarWidth);
        this.legendDiv = this.makeToplevelDiv();
    }

    protected createNewSurfaces(keepColorMap: boolean): void {
        if (this.surface != null)
            this.surface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        this.hps = [];
        this.legendSurface = new HtmlPlottingSurface(this.legendDiv, this.page, {
            height: Resolution.legendSpaceHeight });
        if (keepColorMap)
            this.legendPlot.setSurface(this.legendSurface);
        else
            this.legendPlot = new HistogramLegendPlot(this.legendSurface,
            (xl, xr) => { this.legendSelectionCompleted(xl, xr); });
        this.createAllSurfaces( (surface) => {
            const hp = new Histogram2DPlot(surface);
            this.hps.push(hp);
        });
    }

    public setAxes(xAxisData: AxisData,
                   legendAxisData: AxisData,
                   groupByAxisData: AxisData,
                   relative: boolean): void {
        this.relative = relative;
        this.xAxisData = xAxisData;
        this.legendAxisData = legendAxisData;
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
                if (this.relative)
                    return null;
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
            ranges = [sourceRange, this.legendAxisData.dataRange, this.groupByAxisData.dataRange];
        } else if (eventKind === "YAxis") {
            // TODO
            return;
            /*
            this.relative = false;
            this.updateView(this.data, [0, 0, 0]);
            return;
             */
        } else if (eventKind === "GAxis") {
            ranges = [this.xAxisData.dataRange, this.legendAxisData.dataRange, sourceRange];
        } else {
            return;
        }

        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema, [0, 0, 0],  // any number of buckets
            [this.xAxisData.description, this.legendAxisData.description, this.groupByAxisData.description],
            this.page.title, Converters.eventToString(pageId, eventKind), {
                chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1,
                relative: this.relative, reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public swapAxes(): void {
        const cds = [this.legendAxisData.description,
            this.xAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.legendAxisData.bucketCount, this.buckets, this.shape.bucketCount],
            cds, null, "swap axes",{
                reusePage: true, relative: this.relative,
                chartKind: "Trellis2DHistogram", exact: true
            }));
    }

    protected toggleNormalize(): void {
        this.relative = !this.relative;
        if (this.relative && this.samplingRate < 1) {
            // We cannot use sampling when we display relative views.
            this.exactHistogram();
        } else {
            this.refresh();
        }
    }

    protected onMouseMove(): void {
        const mousePosition = this.mousePosition();
        if (mousePosition.plotIndex == null ||
            mousePosition.x < 0 || mousePosition.y < 0) {
            this.pointDescription.show(false);
            return;
        }

        const plot = this.hps[mousePosition.plotIndex];
        if (plot == null)
            return;

        this.pointDescription.show(true);
        const xs = this.xAxisData.invert(mousePosition.x);
        const y = Math.round(plot.getYScale().invert(mousePosition.y));

        const box = plot.getBoxInfo(mousePosition.x, y);
        const count = (box == null) ? "" : box.count.toString();
        const colorIndex = (box == null) ? null : box.yIndex;
        const value = (box == null) ? "" : this.legendAxisData.bucketDescription(colorIndex, 0);
        const perc = (box == null || box.count === 0) ? 0 : box.count / box.countBelow;
        const group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex, 40);

        // The point description is a child of the canvas, so we use canvas coordinates
        const position = d3mouse(this.surface.getCanvas().node());
        this.pointDescription.update(
            [xs, value.toString(), group, significantDigits(y), percent(perc), count], position[0], position[1]);
        this.legendPlot.highlight(colorIndex);
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
            columnDescription: this.legendAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.groupByAxisData.description,
            isAscending: true
        }]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
    }

    protected exactHistogram(): void {
        const cds = [this.xAxisData.description,
            this.legendAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.buckets, this.legendAxisData.bucketCount, this.shape.bucketCount],
                cds, null, "exact",{
                reusePage: true, relative: this.relative,
                chartKind: "Trellis2DHistogram", exact: true
            }));
    }

    protected chooseBuckets(): void {
        const bucketDialog = new BucketDialog(this.buckets);
        bucketDialog.setAction(() => {
            const ct = bucketDialog.getBucketCount();
            if (ct != null)
                this.updateView(this.data, [ct, this.legendAxisData.bucketCount],
                    this.maxYAxis, true)
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
                this.legendAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [0, 0],
                cds, null, "change groups",{
                    reusePage: true, relative: this.relative,
                    chartKind: "2DHistogram", exact: this.samplingRate >= 1
                }));
        } else {
            const cds = [this.xAxisData.description,
                this.legendAxisData.description, this.groupByAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "Trellis2DHistogram");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
                [0, 0, groupCount],
                cds, null, "change groups",{
                    reusePage: true, relative: this.relative,
                    chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1
                }));
        }
    }

    protected heatmap(): void {
        const cds = [this.xAxisData.description,
            this.legendAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.buckets, this.legendAxisData.bucketCount, this.shape.bucketCount],
            cds, null, this.defaultProvenance,{
                reusePage: false, chartKind: "TrellisHeatmap", exact: true
            }));
    }

    protected export(): void {
        // TODO
    }

    public resize(): void {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        this.shape = TrellisLayoutComputation.resize(chartSize.width, chartSize.height, this.shape);
        this.updateView(this.data, [this.xAxisData.bucketCount, this.legendAxisData.bucketCount],
            this.maxYAxis, true);
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.legendAxisData.description, this.groupByAxisData.description];
        const ranges = [this.xAxisData.dataRange, this.legendAxisData.dataRange, this.groupByAxisData.dataRange];
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema,
            [this.xAxisData.bucketCount, this.legendAxisData.bucketCount, this.groupByAxisData.bucketCount],
            cds, this.page.title, null,{
                chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1,
                relative: this.relative, reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const ser: TrellisHistogram2DSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.legendAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.legendAxisData.bucketCount,
            relative: this.relative,
            groupByColumn: this.groupByAxisData.description,
            xWindows: this.shape.xNum,
            yWindows: this.shape.yNum,
            groupByBucketCount: this.groupByAxisData.bucketCount
        };
        return ser;
    }

    public static reconstruct(ser: TrellisHistogram2DSerialization, page: FullPage): IDataView {
        if (ser.remoteObjectId == null || ser.rowCount == null || ser.xWindows == null ||
            ser.yWindows == null || ser.groupByBucketCount ||
            ser.samplingRate == null || ser.schema == null)
            return null;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const shape = TrellisChartView.deserializeShape(ser, page);
        const view = new TrellisHistogram2DView(ser.remoteObjectId, ser.rowCount,
            schema, shape, ser.samplingRate, page);
        view.setAxes(new AxisData(ser.columnDescription0, null, ser.xBucketCount),
            new AxisData(ser.columnDescription1, null, ser.yBucketCount),
            new AxisData(ser.groupByColumn, null, ser.groupByBucketCount),
            ser.relative);
        return view;
    }

    public updateView(data: Groups<Groups<Groups<number>>>, bucketCount: number[], maxYAxis: number | null, keepColorMap: boolean): void {
        this.createNewSurfaces(keepColorMap);
        this.data = data;
        let max = maxYAxis;
        if (maxYAxis == null) {
            for (let i = 0; i < data.perBucket.length; i++) {
                const buckets = data.perBucket[i];
                for (let j = 0; j < buckets.perBucket.length; j++) {
                    const bj = buckets.perBucket[j];
                    const total = bj.perBucket.reduce(add, 0);
                    if (total > max)
                        max = total;
                }
            }
            this.maxYAxis = max;
        }

        for (let i = 0; i < data.perBucket.length; i++) {
            const buckets = data.perBucket[i];
            const heatmap = { first: buckets, second: null };
            const plot = this.hps[i];
            plot.setData(heatmap, this.xAxisData, this.samplingRate, this.relative,
                this.schema, this.legendPlot.colorMap, max);
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
                this.legendAxisData.getDisplayNameString(this.schema),
                this.groupByAxisData.getDisplayNameString(this.schema),
                "y",
                "percent",
                "count" /* TODO:, "cdf" */], 40);

        this.legendPlot.setData(this.legendAxisData, false /* TODO */, this.schema);
        this.legendPlot.draw();
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
            return new FilterReceiver(title, [this.xAxisData.description, this.legendAxisData.description,
                this.groupByAxisData.description], this.schema, [0, 0, 0], page, operation, this.dataset, {
                chartKind: "Trellis2DHistogram", relative: this.relative,
                reusePage: false, exact: this.samplingRate >= 1
            });
        };
    }

    protected filter(filter: FilterDescription): void {
        if (filter == null)
            return;
        const rr = this.createFilterRequest(filter);
        const title = new PageTitle(this.page.title.format, Converters.filterDescription(filter));
        const renderer = new FilterReceiver(title, [this.xAxisData.description, this.legendAxisData.description,
            this.groupByAxisData.description], this.schema, [0, 0, 0], this.page, rr, this.dataset, {
            chartKind: "Trellis2DHistogram", relative: this.relative,
            reusePage: false, exact: this.samplingRate >= 1
        });
        rr.invoke(renderer);
    }

    protected legendSelectionCompleted(xl: number, xr: number): void {
        [xl, xr] = reorder(xl, xr);
        const x0 = this.legendAxisData.invertToNumber(xl);
        const x1 = this.legendAxisData.invertToNumber(xr);
        if (x0 > x1) {
            this.page.reportError("No data selected");
            return;
        }

        if (d3event.sourceEvent.shiftKey) {
            this.legendPlot.emphasizeRange(xl / this.legendPlot.width, xr / this.legendPlot.width);
            this.resize();
            return;
        }

        const filter: FilterDescription = {
            min: x0,
            max: x1,
            minString: this.legendAxisData.invert(xl),
            maxString: this.legendAxisData.invert(xr),
            cd: this.legendAxisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        this.filter(filter);
    }

    protected selectionCompleted(): void {
        const local = this.selectionIsLocal();
        if (local != null) {
            const origin = this.canvasToChart(this.selectionOrigin);
            const left = this.position(origin.x, origin.y);
            const end = this.canvasToChart(this.selectionEnd);
            const right = this.position(end.x, end.y);
            const [xl, xr] = reorder(left.x, right.x);

            const filter: FilterDescription = {
                min: this.xAxisData.invertToNumber(xl),
                max: this.xAxisData.invertToNumber(xr),
                minString: this.xAxisData.invert(xl),
                maxString: this.xAxisData.invert(xr),
                cd: this.xAxisData.description,
                complement: d3event.sourceEvent.ctrlKey,
            };
            this.filter(filter);
        } else {
            const filter = this.getGroupBySelectionFilter();
            this.filter(filter);
        }
    }
}

/**
 * Renders a Trellis plot of 2D histograms
 */
export class TrellisHistogram2DReceiver extends Receiver<Groups<Groups<Groups<number>>>> {
    protected trellisView: TrellisHistogram2DView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Groups<Groups<Groups<number>>>>,
                protected options: ChartOptions) {
        super(options.reusePage ? page : page.dataset.newPage(title, page), operation, "histogram");
        this.trellisView = new TrellisHistogram2DView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1], axes[2], options.relative);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Groups<Groups<Groups<number>>>>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.trellisView.updateView(value.data, [this.axes[0].bucketCount,
            this.axes[1].bucketCount], null, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.trellisView.updateCompleted(this.elapsedMilliseconds());
    }
}

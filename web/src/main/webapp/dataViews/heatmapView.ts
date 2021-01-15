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

import {event as d3event, mouse as d3mouse} from "d3-selection";
import {HeatmapSerialization, IViewSerialization} from "../datasetView";
import {
    BucketsInfo,
    Groups,
    IColumnDescription,
    kindIsNumeric,
    kindIsString,
    RemoteObjectId,
    RowValue,
} from "../javaBridge";
import {Receiver, RpcRequest} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseReceiver, ChartView, TableTargetAPI} from "../modules";
import {IDataView} from "../ui/dataview";
import {FullPage, PageTitle} from "../ui/fullPage";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {DragEventKind, HtmlString, Resolution} from "../ui/ui";
import {
    assert, assertNever,
    Converters,
    dataRange, Exporter,
    Heatmap,
    ICancellable,
    makeInterval,
    Pair,
    PartialResult,
    reorder,
    Triple,
    Two,
    zip,
} from "../util";
import {AxisData} from "./axisData";
import {DataRangesReceiver, NewTargetReceiver} from "./dataRangesReceiver";
import {Dialog, FieldKind, saveAs} from "../ui/dialog";
import {ColorMapKind, HeatmapLegendPlot} from "../ui/heatmapLegendPlot";
import {HistogramLegendPlot} from "../ui/histogramLegendPlot";
import {TableMeta} from "../ui/receiver";

/**
 * A HeatMapView renders information as a heatmap.
 */
export class HeatmapView extends
    ChartView<Triple<Groups<Groups<number>>,
                     Groups<Groups<number>> | null,
                     Groups<Groups<RowValue[]>> | null>> {
    // Data has the following structure:
    // first: heatmap data, second: confidence data, third: optional row corresponding to cells with 1 values,
    // with one cell for each column in the detailColumns schema.

    protected colorLegend: HeatmapLegendPlot;
    protected detailLegend: HistogramLegendPlot | null;
    protected plot: HeatmapPlot;
    protected legendSurface: HtmlPlottingSurface;
    protected detailLegendDiv: HTMLDivElement | null;
    protected detailLegendSurface: HtmlPlottingSurface | null;
    protected xPoints: number;
    protected yPoints: number;
    protected readonly viewMenu: SubMenu;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected detailsAxisData: AxisData | null;
    protected legendDiv: HTMLDivElement;
    private readonly defaultProvenance: string = "From heatmap";
    private showDetails: boolean;  // true if we display data about 1-sized boxes
    private detailIndex: number;  // column that is used to display the detail colormap

    constructor(remoteObjectId: RemoteObjectId,
                meta: TableMeta,
                // columns that are displayed when hovering over boxes with count 1.
                // null when displaying private data.  First two columns are the X and Y axes.
                protected detailColumns: SchemaClass | null,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, meta, page, "Heatmap");
        if (detailColumns != null)
            assert(detailColumns.length >= 2);
        this.detailLegendSurface = null;
        this.detailLegendDiv = null;
        this.detailLegend = null;
        this.showDetails = false;
        this.detailIndex = 2;
        this.viewMenu = new SubMenu([
            {
                text: "refresh",
                action: () => {
                    this.refresh();
                },
                help: "Redraw this view.",
            }, {
                text: "swap axes",
                action: () => this.swapAxes(),
                help: "Draw the heatmap with the same data by swapping the X and Y axes.",
            },  { text: "# buckets...",
                action: () => this.chooseBuckets(),
                help: "Change the number of buckets used to draw this histogram. ",
            }, {
                text: "table",
                action: () => this.showTable([this.xAxisData.description!,
                     this.yAxisData.description!], this.defaultProvenance),
                help: "View the data underlying this view as a table.",
            }, {
                text: "histogram",
                action: () => this.histogram(),
                help: "Show this data as a two-dimensional histogram.",
            }, {
                text: "Quartiles vector",
                action: () => this.quartileView(),
                help: "Show this data as a vector of quartiles.",
            }, {
                text: "group by...",
                action: () => this.groupBy(),
                help: "Group data by a third column.",
            }, {
                text: "Show/hide regression",
                action: () => this.toggleRegression(),
                help: "Show or hide the linear regression line"
            }, {
                text: "Change detail column...",
                action: () => this.changeDetailsColumn(),
                help: "Change the column that is used for displaying the second color map."
            }
        ]);
        this.menu = new TopMenu([
            this.exportMenu(),
            {text: "View", help: "Change the way the data is displayed.", subMenu: this.viewMenu},
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.viewMenu.enable("Change detail column...",
            this.detailColumns != null && this.detailColumns.length >= 2);
        this.page.setMenu(this.menu);
        this.detailLegendDiv = this.makeToplevelDiv("detailLegend");
        this.createDiv("legend");
        this.createDiv("chart");
        this.createDiv("summary");
    }

    public changeDetailsColumn(): void {
        if (this.detailColumns == null)
            return;
        const dialog = new Dialog("Details column",
            "Select the column used to display the second color map.");
        let input = dialog.addColumnSelectField("column", "Column:",
            this.detailColumns.allColumnNames().slice(2), null,
            "Data in this column will be used for the second color map.");
        input.required = true;
        dialog.setAction(() => {
            const c = dialog.getColumnName("column");
            this.detailIndex = this.detailColumns!.columnIndex(c);
            this.resize();
        });
        dialog.show();
    }

    private quartileView(): void {
        if (!kindIsNumeric(this.yAxisData.description!.kind)) {
            this.page.reportError("Quartiles require a numeric second column");
            return;
        }
        const cds = [this.xAxisData.description!, this.yAxisData.description!];
        const rr = this.createDataQuantilesRequest(cds, this.page, "QuartileVector");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0], cds, null, this.defaultProvenance,{
                reusePage: false,
                chartKind: "QuartileVector",
            }));
    }

    public toggleRegression(): void {
        this.plot.toggleRegression();
    }

    public chooseBuckets(): void {
        const bucketDialog = new Dialog("Set buckets",
            "Change the resolution used to display the heatmap.");

        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        let input = bucketDialog.addTextField("x_buckets", "X axis buckets:", FieldKind.Integer, null,
            "The number of buckets on X axis.");
        input.min = "1";
        input.max = Math.floor(chartSize.width / Resolution.minDotSize).toString();
        input.value = this.xAxisData.bucketCount.toString();
        input.required = true;

        input = bucketDialog.addTextField("y_buckets", "Y axis buckets:", FieldKind.Integer, null,
            "The number of buckets on Y axis.");
        input.min = "1";
        input.max = Math.floor(chartSize.height / Resolution.minDotSize).toString();
        input.value = this.yAxisData.bucketCount.toString();
        input.required = true;
        bucketDialog.setAction(() => this.changeBuckets(
            bucketDialog.getFieldValueAsInt("x_buckets"),
            bucketDialog.getFieldValueAsInt("y_buckets")));
        bucketDialog.show();
    }

    private changeBuckets(x: number | null, y: number | null): void {
        if (x == null || y == null) {
            this.page.reportError("Illegal value");
            return;
        }
        const rr = this.createDataQuantilesRequest(
            this.getColumns(),
            this.page, "Heatmap");
        rr.invoke(new DataRangesReceiver(
            this, this.page, rr, this.meta, [x, y],
            this.getColumns(), null,
            this.defaultProvenance, { chartKind: "Heatmap", exact: true, reusePage: true }));
    }

    protected createNewSurfaces(keepColorMap: boolean): void {
        if (this.detailLegendSurface != null)
            this.detailLegendSurface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        if (this.surface != null)
            this.surface.destroy();

        if (this.showDetails)
            this.detailLegendSurface = new HtmlPlottingSurface(this.detailLegendDiv!, this.page,
                { height: Resolution.legendSpaceHeight });
        this.legendSurface = new HtmlPlottingSurface(this.legendDiv, this.page,
            { height: Resolution.legendSpaceHeight * 2 / 3 });
        if (keepColorMap) {
            this.colorLegend.moved();
            this.colorLegend.setSurface(this.legendSurface);
            if (this.showDetails) {
                this.detailLegend!.moved();
                this.detailLegend!.setSurface(this.detailLegendSurface!);
            }
        } else {
            this.colorLegend = new HeatmapLegendPlot(
                this.legendSurface,
                (xl, xr) => this.legendSelectionCompleted(xl, xr));
            this.colorLegend.setColorMapChangeEventListener(
                () => this.updateView(this.data, true));
            if (this.showDetails) {
                // noinspection JSUnusedLocalSymbols
                this.detailLegend = new HistogramLegendPlot(
                    this.detailLegendSurface!,
                    (xl, xr) => this.detailSelectionCompleted(xl, xr));
            }
        }

        this.surface = new HtmlPlottingSurface(this.chartDiv!, this.page,
            { topMargin: 20, leftMargin: Resolution.heatmapLabelWidth });
        this.plot = new HeatmapPlot(
            // The color maps may not be yet set, but they will be by the time we use them.
            this.surface, this.colorLegend?.getColorMap(),
            this.detailLegend == null ? null : this.detailLegend.getColorMap(),
            this.detailIndex, true);
    }

    protected legendSelectionCompleted(xl: number, xr: number): void {
        const [min, max] = reorder(this.colorLegend.invert(xl), this.colorLegend.invert(xr));
        const h = Heatmap.create(this.data.first);
        const bitmap = h.bitmap((c) => min <= c && c <= max);
        const bucketCount = bitmap.sum();
        const filter = h.map((c) => min <= c && c <= max ? c : 0);
        const pointCount = filter.sum();
        const shiftPressed = d3event.sourceEvent.shiftKey;
        if (this.summary != null) {
            this.summary.set("buckets selected", bucketCount);
            this.summary.set("points selected", pointCount);
            this.summary.display();
        }
        if (shiftPressed) {
            this.colorLegend.emphasizeRange(xl, xr);
        }
    }

    protected detailSelectionCompleted(xl: number, xr: number): void {
        const shiftPressed = d3event.sourceEvent.shiftKey;
        if (shiftPressed) {
            let min = this.detailsAxisData!.bucketCount * xl / this.detailLegend!.width;
            let max = this.detailsAxisData!.bucketCount * xr / this.detailLegend!.width;
            [min, max] = reorder(min, max);
            this.detailLegend!.emphasizeRange(min, max);
            this.updateView(this.data, true)
        }
    }

    public setAxes(xData: AxisData, yData: AxisData): void {
        this.xAxisData = xData;
        this.yAxisData = yData;
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
            case "GAxis":
                return null;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                return this.yAxisData;
            default:
                assertNever(event);
        }
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.data === null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        let ranges = [];
        if (eventKind === "XAxis") {
            ranges = [sourceRange, this.yAxisData.dataRange];
        } else if (eventKind === "YAxis") {
            ranges = [this.xAxisData.dataRange, sourceRange];
        } else {
            return;
        }

        const receiver = new DataRangesReceiver(this,
            this.page, null, this.meta, [0, 0],  // any number of buckets
            this.getColumns(), null,
            Converters.eventToString(pageId, eventKind), {
                chartKind: "Heatmap", exact: this.samplingRate >= 1,
                relative: false, pieChart: false, reusePage: true
            });
        receiver.run(ranges);
        receiver.finished();
    }

    public updateView(data: Triple<Groups<Groups<number>>,
                                   Groups<Groups<number>> | null,
                                   Groups<Groups<RowValue[]>> | null>,
                      keepColorMap: boolean): void {
        this.showDetails = false;
        if (data?.first === null || data.first.perBucket.length === 0) {
            this.page.reportError("No data to display");
            return;
        }

        this.data = data;
        this.xPoints = data.first.perBucket.length;
        this.yPoints = data.first.perBucket[0].perBucket.length;
        if (this.yPoints === 0) {
            this.page.reportError("No data to display");
            return;
        }

        this.detailsAxisData = null;
        let detailLegendColumn: IColumnDescription | null = null;
        let range: BucketsInfo | null = null;
        if (this.data.third != null) {
            assert(this.detailColumns != null);
            if (this.detailColumns.schema.length <= 2) {
                this.showDetails = false;
            } else {
                detailLegendColumn = this.detailColumns.schema[this.detailIndex];
                const detailData: RowValue[] = this.data.third.perBucket.reduce(
                    (v: RowValue[], b) => v.concat(b.perBucket.map(r => r != null ? r[this.detailIndex] : null)), []);
                range = dataRange(detailData, detailLegendColumn);
                this.showDetails = range.presentCount > 0;
                let legendBucketCount: number;
                // noinspection JSObjectNullOrUndefined
                if (kindIsString(detailLegendColumn.kind)) {
                    // noinspection JSObjectNullOrUndefined
                    legendBucketCount = range.stringQuantiles!.length;
                } else if (detailLegendColumn.kind === "Integer") {
                    // noinspection JSObjectNullOrUndefined
                    legendBucketCount = Math.min(range.max! - range.min! + 1, Resolution.max2DBucketCount);
                } else {
                    legendBucketCount = Resolution.max2DBucketCount;
                }
                this.detailsAxisData = new AxisData(detailLegendColumn, range, legendBucketCount);
            }
        }

        this.createNewSurfaces(keepColorMap);
        if (this.isPrivate()) {
            const cols = [this.xAxisData.description.name, this.yAxisData.description.name];
            const eps = this.dataset.getEpsilon(cols);
            this.page.setEpsilon(eps, cols);
        }

        // The order of these operations is important:
        // Data must be set in each legend, then the kind, then it can be drawn.
        // The heatmap itself can only be drawn after the colormaps have been set.
        this.plot.setData(data, this.xAxisData, this.yAxisData, this.detailsAxisData,
            this.meta.schema, this.isPrivate());
        if (!keepColorMap) {
            this.colorLegend.setData(this.plot.getMaxCount());
            if (this.showDetails)
                this.colorLegend.setColorMapKind(ColorMapKind.Grayscale);
        }
        this.colorLegend.draw();
        if (this.showDetails) {
            if (!keepColorMap)
                this.detailLegend!.setData(this.detailsAxisData!, false);
            this.detailLegend!.draw();
        }
        this.plot.draw();

        this.setupMouse();
        const cols = this.getColumns().map((c) => c.name);
        cols.splice(2, 0, "count");
        assert(this.surface != null);
        this.pointDescription = new TextOverlay(
            this.surface.getChart(),
            this.surface.getActualChartSize(),
            cols, 40);
        this.standardSummary();
        assert(this.summary != null);
        this.summary.set("data points", this.plot.getVisiblePoints());
        this.summary.set("distinct dots", this.plot.getDistinct());
        if (this.samplingRate < 1.0) {
            this.summary.set("sampling rate", this.samplingRate);
        }
        this.summary.setString("pixel width", new HtmlString(this.xAxisData.barWidth()));
        this.summary.setString("pixel height", new HtmlString(this.yAxisData.barWidth()));
        this.summary.display();
    }

    public serialize(): IViewSerialization {
        const ser = super.serialize();
        // noinspection UnnecessaryLocalVariableJS
        const result: HeatmapSerialization = {
            detailedColumns: this.detailColumns != null ? this.detailColumns.serialize() : null,
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.yAxisData.bucketCount,
            xRange: this.xAxisData.dataRange,
            yRange: this.yAxisData.dataRange,
            ...ser,
        };
        return result;
    }

    public static reconstruct(ser: HeatmapSerialization, page: FullPage): IDataView | null {
        if (ser.columnDescription0 === null || ser.columnDescription1 === null ||
            ser.samplingRate === null || ser.xBucketCount === null || ser.yBucketCount === null ||
            ser.xRange === null || ser.yRange === null) {
            return null;
        }
        const args = this.validateSerialization(ser);
        if (args == null)
            return null;
        const detailed: SchemaClass | null = ser.detailedColumns != null ?
            new SchemaClass([]).deserialize(ser.detailedColumns) : null;
        const hv = new HeatmapView(
            ser.remoteObjectId, args, detailed, ser.samplingRate, page);
        hv.setAxes(
            new AxisData(ser.columnDescription0, ser.xRange, ser.xBucketCount),
            new AxisData(ser.columnDescription1, ser.yRange, ser.yBucketCount));
        return hv;
    }

    // Draw this as a 2-D histogram
    public histogram(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0], cds, null, this.defaultProvenance, {
            reusePage: false,
            chartKind: "2DHistogram",
            exact: true, stacked: true
        }));
    }

    public groupBy(): void {
        const columns: string[] = this.getSchema().namesExcluding(
            [this.xAxisData.description.name, this.yAxisData.description.name]);
        this.chooseTrellis(columns);
    }

    public export(): void {
        const lines: string[] = Exporter.histogram2DAsCsv(
            this.data.first, this.getSchema(), [this.xAxisData, this.yAxisData]);
        const fileName = "heatmap.csv";
        saveAs(fileName, lines.join("\n"));
    }

    protected showTrellis(colName: string): void {
        const groupBy = this.getSchema().find(colName);
        if (groupBy == null)
            return;
        const cds: IColumnDescription[] = [this.xAxisData.description,
                                           this.yAxisData.description, groupBy];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0, 0], cds, null, this.defaultProvenance,{
            reusePage: false, chartKind: "TrellisHeatmap", exact: true
        }));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new NewTargetReceiver(title, this.getColumns(),
                this.meta, [0, 0], page, operation, this.dataset, {
                exact: true, chartKind: "Heatmap", reusePage: false,
            });
        };
    }

    public getColumns(): IColumnDescription[] {
        if (this.detailColumns != null)
            return this.detailColumns.schema;
        return [this.xAxisData.description, this.yAxisData.description];
    }

    public swapAxes(): void {
        const cds: IColumnDescription[] = this.getColumns();
        cds[0] = this.yAxisData.description;
        cds[1] = this.xAxisData.description;
        const rr = this.createDataQuantilesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0], cds, null, "swap axes",{
            chartKind: "Heatmap",
            exact: this.samplingRate >= 1,
            reusePage: true
        }));
    }

    public refresh(): void {
        const ranges = [this.xAxisData.dataRange, this.yAxisData.dataRange];
        const receiver = new DataRangesReceiver(this,
            this.page, null, this.meta, [this.xAxisData.bucketCount, this.yAxisData.bucketCount],
            this.getColumns(), this.page.title, null,{
                chartKind: "Heatmap", exact: this.samplingRate >= 1, reusePage: true,
            });
        receiver.run(ranges);
        receiver.finished();
    }

    public resize(): void {
        if (this.data == null)
            return;
        this.updateView(this.data, true);
    }

    protected onMouseMove(): void {
        if (this.xAxisData.axis == null) {
            // not yet setup
            return;
        }

        const position = d3mouse(this.surface!.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];
        let data: string[] | null = null;

        if (this.showDetails) {
            const index = this.plot.getBucketIndex(mouseX, mouseY);
            if (index != null) {
                const d = this.data.third!.perBucket[index[0]].perBucket[index[1]];
                if (d != null) {
                    // We will use the information from the point itself for the first two
                    // columns.
                    data = zip(d, this.detailColumns!.schema,
                        (v, desc) => Converters.valueToString(v, desc.kind, false));
                    data.splice(2, 0, "1"); // We know that the count is 1.
                    const value = d[this.detailIndex];
                    const valueIndex = this.detailsAxisData!.bucketIndex(value);
                    this.detailLegend!.showBorder(valueIndex);
                } else {
                    this.detailLegend!.showBorder(null);
                }
            }
        }

        if (data == null) {
            const xs = this.xAxisData.invert(mouseX);
            const ys = this.yAxisData.invert(mouseY);
            const value = this.plot.getCount(mouseX, mouseY);
            this.colorLegend.highlight(value[0], value[1]);
            data = [xs, ys, makeInterval(value)];
        }
        this.pointDescription!.update(data, mouseX, mouseY);
    }

    protected dragStart(): void {
        this.dragStartRectangle();
    }

    protected dragMove(): boolean {
        return this.dragMoveRectangle();
    }

    protected dragEnd(): boolean {
        if (!super.dragEnd() || this.selectionOrigin == null)
            return false;
        const position = d3mouse(this.surface!.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
        return true;
    }

    private selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        const f = this.filterSelectionRectangle(xl, xr, yl, yr, this.xAxisData, this.yAxisData);
        if (f == null)
            return;
        const rr = this.createFilterRequest(f);
        const renderer = new NewTargetReceiver(new PageTitle(this.page.title.format,
            Converters.filterArrayDescription(f)),
            this.getColumns(),
            this.meta, [0, 0], this.page, rr, this.dataset, {
            exact: this.samplingRate >= 1, chartKind: "Heatmap", reusePage: false,
        });
        rr.invoke(renderer);
    }
}

/**
 * Receives a heatmap given data and confidences.
 */
export class HeatmapReceiver extends Receiver<Two<Groups<Groups<number>>>> {
    protected view: HeatmapView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected meta: TableMeta,
                protected axisData: AxisData[],
                protected samplingRate: number,
                operation: RpcRequest<Two<Groups<Groups<number>>>>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset!.newPage(title, page), operation, "histogram");
        this.view = new HeatmapView(
            remoteTable.getRemoteObjectId()!, meta, null,
            this.samplingRate, this.page);
        this.view.setAxes(axisData[0], axisData[1]);
    }

    public onNext(value: PartialResult<Two<Groups<Groups<number>>>>): void {
        super.onNext(value);
        if (value == null)
            return;
        this.view.updateView({ ...value.data, third: null }, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

/**
 * Receives a heatmap and data for each cell which has a count of 1.
 */
export class HeatmapWithDataReceiver extends Receiver<Pair<Groups<Groups<number>>, Groups<Groups<RowValue[]>>>> {
    protected view: HeatmapView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected meta: TableMeta,
                protected detailedColumns: SchemaClass,
                protected axisData: AxisData[],
                protected samplingRate: number,
                operation: RpcRequest<Pair<Groups<Groups<number>>, Groups<Groups<RowValue[]>>>>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset!.newPage(title, page), operation, "histogram");
        this.view = new HeatmapView(
            remoteTable.getRemoteObjectId()!, meta, detailedColumns,
            this.samplingRate, this.page);
        this.view.setAxes(axisData[0], axisData[1]);
    }

    public onNext(value: PartialResult<Pair<Groups<Groups<number>>, Groups<Groups<RowValue[]>>>>): void {
        super.onNext(value);
        if (value === null)
            return;
        const data = value.data;
        this.view.updateView({ first: data.first, second: null, third: data.second }, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

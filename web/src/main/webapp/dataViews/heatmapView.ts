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

import {drag as d3drag} from "d3-drag";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {HeatmapSerialization, IViewSerialization} from "../datasetView";
import {DistinctStrings} from "../distinctStrings";
import {
    BasicColStats, CombineOperators, FilterDescription, HeatMap,
    Histogram2DArgs, IColumnDescription, kindIsNumeric, RecordOrder, RemoteObjectId,
} from "../javaBridge";
import { Receiver } from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BigTableView, TableTargetAPI, ZipReceiver} from "../tableTarget";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {HistogramPlot} from "../ui/histogramPlot";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {Point, Resolution} from "../ui/ui";
import {
    formatNumber, ICancellable, Pair, PartialResult, reorder,
    saveAs, Seed, significantDigits,
} from "../util";
import {AxisData} from "./axisData";
import {Filter2DReceiver, Histogram2DRenderer, Make2DHistogram} from "./histogram2DView";
import {HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {ChartObserver} from "./tsViewBase";

/**
 * A HeatMapView renders information as a heatmap.
 */
export class HeatmapView extends BigTableView {
    protected dragging: boolean;
    /**
     * Coordinates of mouse within canvas.
     */
    private selectionOrigin: Point;
    private selectionRectangle: any;
    protected colorLegend: HeatmapLegendPlot;
    protected summary: HTMLElement;
    private moved: boolean;
    protected pointDescription: TextOverlay;
    protected surface: PlottingSurface;
    protected plot: HeatmapPlot;
    protected showMissingData: boolean = false;  // TODO: enable this
    protected xHistoSurface: PlottingSurface;
    protected xHistoPlot: HistogramPlot;

    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        heatMap: HeatMap;
        xPoints: number;
        yPoints: number;
        samplingRate: number;
    };
    private readonly menu: TopMenu;
    protected readonly viewMenu: SubMenu;

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Heatmap");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = (e) => this.keyDown(e);
        this.dragging = false;
        this.moved = false;
        this.viewMenu = new SubMenu([
            {
                text: "refresh",
                action: () => {
                    this.refresh();
                },
                help: "Redraw this view.",
            },
            {
                text: "swap axes",
                action: () => {
                    this.swapAxes();
                },
                help: "Draw the heatmap with the same data by swapping the X and Y axes.",
            },
            {
                text: "table",
                action: () => {
                    this.showTable();
                },
                help: "View the data underlying this view as a table.",
            },
            {
                text: "histogram",
                action: () => {
                    this.histogram();
                },
                help: "Show this data as a two-dimensional histogram.",
            },
            {
                text: "group by",
                action: () => {
                    this.trellis();
                },
                help: "Group data by a third column.",
            },
        ]);
        this.menu = new TopMenu([
            {
                text: "Export",
                help: "Save the information in this view in a local file.",
                subMenu: new SubMenu([{
                    text: "As CSV",
                    help: "Saves the data in this view in a CSV file.",
                    action: () => {
                        this.export();
                    },
                }]),
            },
            {text: "View", help: "Change the way the data is displayed.", subMenu: this.viewMenu},
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.topLevel.tabIndex = 1;

        const legendSurface = new PlottingSurface(this.topLevel, page);
        // legendSurface.setMargins(0, 0, 0, 0);
        legendSurface.setHeight(Resolution.legendSpaceHeight * 2 / 3);
        this.colorLegend = new HeatmapLegendPlot(legendSurface);
        this.colorLegend.setColorMapChangeEventListener(
            () => this.updateView(
                    this.currentData.heatMap,
                    this.currentData.xData,
                    this.currentData.yData,
                    this.currentData.samplingRate,
                    true,
                    0));

        this.surface = new PlottingSurface(this.topLevel, page);
        this.surface.setMargins(20, this.surface.rightMargin, this.surface.bottomMargin, this.surface.leftMargin);
        this.plot = new HeatmapPlot(this.surface, this.colorLegend);

        if (this.showMissingData) {
            this.xHistoSurface = new PlottingSurface(this.topLevel, page);
            this.xHistoSurface.setMargins(0, null, 16, null);
            this.xHistoSurface.setHeight(100);
            this.xHistoPlot = new HistogramPlot(this.xHistoSurface);
        }

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    public updateView(heatmap: HeatMap, xData: AxisData, yData: AxisData,
                      samplingRate: number, keepColorMap: boolean, elapsedMs: number): void {
        this.page.reportTime(elapsedMs);
        if (!keepColorMap) {
            this.colorLegend.clear();
        }
        this.plot.clear();
        if (this.showMissingData) {
            this.xHistoPlot.clear();
        }
        if (heatmap == null || heatmap.buckets.length === 0) {
            this.page.reportError("No data to display");
            return;
        }

        // TODO: this should go away
        if (!kindIsNumeric(xData.description.kind) || !kindIsNumeric(yData.description.kind)) {
            this.viewMenu.enable("group by", false);
        }

        const xPoints = heatmap.buckets.length;
        const yPoints = heatmap.buckets[0].length;
        if (yPoints === 0) {
            this.page.reportError("No data to display");
            return;
        }

        this.currentData = {
            heatMap: heatmap,
            xData: xData,
            yData: yData,
            xPoints: xPoints,
            yPoints: yPoints,
            samplingRate: samplingRate,
        };

        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        const canvas = this.surface.getCanvas();
        canvas.call(drag)
            .on("mousemove", () => this.onMouseMove())
            .on("mouseenter", () => this.onMouseEnter())
            .on("mouseleave", () => this.onMouseLeave());

        // The order of these operations is important
        this.plot.setData(heatmap, xData, yData, samplingRate);
        if (!keepColorMap) {
            this.colorLegend.setData(1, this.plot.getMaxCount());
        }
        this.colorLegend.draw();
        this.plot.draw();
        /*
        let margin = this.plot.labelWidth();
        if (margin > this.surface.leftMargin) {
            this.surface.setMargins(null, null, null, margin);
            this.surface.moveCanvas();
        }
        */
        if (this.showMissingData) {
            this.xHistoPlot.setHistogram(heatmap.histogramMissingX, 1.0, xData);
            this.xHistoPlot.draw();
        }

        this.selectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);

        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getDefaultChartSize(),
            [xData.description.name, yData.description.name, "count"], 40);
        this.pointDescription.show(false);
        let summary = formatNumber(this.plot.getVisiblePoints()) + " data points";
        if (heatmap.missingData !== 0) {
            summary += ", " + formatNumber(heatmap.missingData) + " missing";
        }
        if (heatmap.histogramMissingX.missingData !== 0) {
            summary += ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate";
        }
        if (heatmap.histogramMissingY.missingData !== 0) {
            summary += ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate";
        }
        summary += ", " + formatNumber(this.plot.getDistinct()) + " distinct dots";
        if (samplingRate < 1.0) {
            summary += ", sampling rate " + significantDigits(samplingRate);
        }
        this.summary.innerHTML = summary;
    }

    public serialize(): IViewSerialization {
        const ser = super.serialize();
        const result: HeatmapSerialization = {
            exact: this.currentData.samplingRate >= 1,
            columnDescription0: this.currentData.xData.description,
            columnDescription1: this.currentData.yData.description,
            ...ser,
        };
        return result;
    }

    public static reconstruct(ser: HeatmapSerialization, page: FullPage): IDataView {
        const exact: boolean = ser.exact;
        const cd0: IColumnDescription = ser.columnDescription0;
        const cd1: IColumnDescription = ser.columnDescription1;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd0 == null || cd1 == null || exact == null || schema == null) {
            return null;
        }
        const cds = [cd0, cd1];

        const hv = new HeatmapView(ser.remoteObjectId, ser.rowCount, schema, page);
        const rr = page.dataset.createGetCategoryRequest(page, cds);
        rr.invoke(new ChartObserver(hv, page, rr, null,
            ser.rowCount, schema,
            { exact, heatmap: true, relative: false, reusePage: true }, cds));
        return hv;
    }

    // Draw this as a 2-D histogram
    public histogram(): void {
        const rcol = new Range2DCollector(
            [this.currentData.xData.description, this.currentData.yData.description],
            this.rowCount,
            this.schema,
            [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
            this.page, this, this.currentData.samplingRate >= 1, null, false, false, false);
        rcol.setValue({ first: this.currentData.xData.stats, second: this.currentData.yData.stats });
        rcol.onCompleted();
    }

    public trellis(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.kind === "Category" && col.name !== this.currentData.xData.description.name
                && col.name !== this.currentData.yData.description.name) {
                columns.push(this.schema.displayName(col.name));
            }
        }
        if (columns.length === 0) {
            this.page.reportError("No acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column", "Select a column to group on.");
        dialog.addSelectField("column", "column", columns, null,
            "The categorical column that will be used to group on.");
        dialog.setAction(() => this.showTrellis(dialog.getFieldValue("column")));
        dialog.show();
    }

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "heatmap.csv";
        saveAs(fileName, lines.join("\n"));
        this.page.reportError("Check the downloads folder for a file named '" + fileName + "'");
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [
            JSON.stringify(this.currentData.xData.description.name + "_min") + "," +
            JSON.stringify(this.currentData.xData.description.name + "_max") + "," +
            JSON.stringify(this.currentData.xData.description.name) + "," +
            JSON.stringify(this.currentData.yData.description.name + "_min") + "," +
            JSON.stringify(this.currentData.yData.description.name + "_max") + "," +
            JSON.stringify(this.currentData.yData.description.name) + "," +
                "count",
        ];
        for (let x = 0; x < this.currentData.heatMap.buckets.length; x++) {
            const data = this.currentData.heatMap.buckets[x];
            const bx = this.currentData.xData.boundaries(x);
            const bdx = JSON.stringify(this.currentData.xData.bucketDescription(x));
            for (let y = 0; y < data.length; y++) {
                if (data[y] === 0) {
                    continue;
                }
                const by = this.currentData.yData.boundaries(y);
                const bdy = JSON.stringify(this.currentData.yData.bucketDescription(y));
                const line = bx[0].toString() + "," + bx[1].toString() + "," + bdx + "," +
                    by[0].toString() + "," + by[1].toString() + "," + bdy + "," + data[y];
                lines.push(line);
            }
        }
        return lines;
    }

    private showTrellis(colName: string) {
        const groupBy = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.currentData.xData.description,
                                         this.currentData.yData.description, groupBy];
        const rr = this.page.dataset.createGetCategoryRequest(this.page, cds);
        rr.invoke(new ChartObserver(this, this.page, rr,
            null, this.rowCount, this.schema,
            { exact: false, heatmap: true, relative: false, reusePage: false}, cds));
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        const r = this.dataset.getSelected();
        if (r.first == null) {
            return;
        }

        const rr = this.createZipRequest(r.first);
        const renderer = (page: FullPage, operation: ICancellable) => {
            return new Make2DHistogram(
                page, operation,
                [this.currentData.xData.description, this.currentData.yData.description],
                [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
                this.rowCount, this.schema, this.currentData.samplingRate >= 1,
                true, this.dataset, false);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.dataset, renderer));
    }

    // show the table corresponding to the data in the heatmap
    public showTable(): void {
        const order =  new RecordOrder([ {
            columnDescription: this.currentData.xData.description,
            isAscending: true,
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true,
        }]);
        const page = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page);
        page.setDataView(table);
        table.schema = this.schema;
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code === "Escape") {
            this.cancelDrag();
        }
    }

    protected cancelDrag() {
        this.dragging = false;
        this.moved = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    public swapAxes(): void {
        const collector = new Range2DCollector(
            [this.currentData.yData.description, this.currentData.xData.description],
            this.rowCount,
            this.schema,
            [this.currentData.yData.distinctStrings, this.currentData.xData.distinctStrings],
            this.page, this, this.currentData.samplingRate >= 1, null, true, false, false);
        collector.setValue( {
            first: this.currentData.yData.stats,
            second: this.currentData.xData.stats });
        collector.onCompleted();
    }

    public refresh(): void {
        if (this.currentData == null) {
            return;
        }
        this.updateView(
            this.currentData.heatMap,
            this.currentData.xData,
            this.currentData.yData,
            this.currentData.samplingRate,
            true,
            0);
    }

    protected onMouseEnter(): void {
        if (this.pointDescription != null) {
            this.pointDescription.show(true);
        }
    }

    protected onMouseLeave(): void {
        if (this.pointDescription != null) {
            this.pointDescription.show(false);
        }
    }

    public onMouseMove(): void {
        if (this.plot.xScale == null) {
            // not yet setup
            return;
        }

        const position = d3mouse(this.surface.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];

        const xs = HistogramViewBase.invert(mouseX, this.plot.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.distinctStrings);
        const ys = HistogramViewBase.invert(mouseY, this.plot.yScale,
            this.currentData.yData.description.kind, this.currentData.yData.distinctStrings);

        const value = this.plot.getCount(mouseX, mouseY);
        this.pointDescription.update([xs, ys, value.toString()], mouseX, mouseY);
    }

    public dragStart(): void {
        this.dragging = true;
        this.moved = false;
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    public dragMove(): void {
        this.onMouseMove();
        if (!this.dragging) {
            return;
        }
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let oy = this.selectionOrigin.y;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        let width = x - ox;
        let height = y - oy;

        if (width < 0) {
            ox = x;
            width = -width;
        }
        if (height < 0) {
            oy = y;
            height = -height;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", oy)
            .attr("width", width)
            .attr("height", height);
    }

    public dragEnd(): void {
        if (!this.dragging || !this.moved) {
            return;
        }
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
    }

    /**
     * Selection has been completed.  The mouse coordinates are within the canvas.
     */
    public selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        yl -= this.surface.topMargin;
        yr -= this.surface.topMargin;

        if (this.plot.xScale == null || this.plot.yScale == null) {
            return;
        }

        let xMin = HistogramViewBase.invertToNumber(xl, this.plot.xScale, this.currentData.xData.description.kind);
        let xMax = HistogramViewBase.invertToNumber(xr, this.plot.xScale, this.currentData.xData.description.kind);
        let yMin = HistogramViewBase.invertToNumber(yl, this.plot.yScale, this.currentData.yData.description.kind);
        let yMax = HistogramViewBase.invertToNumber(yr, this.plot.yScale, this.currentData.yData.description.kind);
        [xMin, xMax] = reorder(xMin, xMax);
        [yMin, yMax] = reorder(yMin, yMax);

        let xBoundaries: string[] = null;
        let yBoundaries: string[] = null;
        if (this.currentData.xData.distinctStrings != null) {
            xBoundaries = this.currentData.xData.distinctStrings.categoriesInRange(xMin, xMax, xMax - xMin);
        }
        if (this.currentData.yData.distinctStrings != null) {
            yBoundaries = this.currentData.yData.distinctStrings.categoriesInRange(yMin, yMax, yMax - yMin);
        }
        const xRange: FilterDescription = {
            min: xMin,
            max: xMax,
            kind: this.currentData.xData.description.kind,
            columnName: this.currentData.xData.description.name,
            bucketBoundaries: xBoundaries,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const yRange: FilterDescription = {
            min: yMin,
            max: yMax,
            kind: this.currentData.yData.description.kind,
            columnName: this.currentData.yData.description.name,
            bucketBoundaries: yBoundaries,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilter2DRequest(xRange, yRange);
        const renderer = new Filter2DReceiver(
            this.currentData.xData.description, this.currentData.yData.description,
            this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings,
            this.rowCount, this.schema, this.page, this.currentData.samplingRate >= 1, rr, true,
            this.dataset, false);
        rr.invoke(renderer);
    }
}

/**
 * Waits for all column stats to be received and then initiates a heatmap or 2D histogram.
 */
export class Range2DCollector extends Receiver<Pair<BasicColStats, BasicColStats>> {
    protected stats: Pair<BasicColStats, BasicColStats>;
    constructor(protected cds: IColumnDescription[],
                protected rowCount: number,
                protected schema: SchemaClass,
                protected ds: DistinctStrings[],
                page: FullPage,
                protected remoteObject: TableTargetAPI,
                protected exact: boolean,
                operation: ICancellable,
                protected drawHeatMap: boolean,  // true - heatMap, false - histogram
                protected relative: boolean,
                protected reusePage: boolean,
    ) {
        super(page, operation, "range2d");
    }

    public setValue(bcs: Pair<BasicColStats, BasicColStats>): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: BigTableView) {
        this.remoteObject = ro;
    }

    public onNext(value: PartialResult<Pair<BasicColStats, BasicColStats>>): void {
        super.onNext(value);
        this.setValue(value.data);

        HistogramViewBase.adjustStats(this.cds[0].kind, this.stats.first);
        HistogramViewBase.adjustStats(this.cds[1].kind, this.stats.second);
    }

    public draw(): void {
        const xBucketCount = HistogramViewBase.bucketCount(this.stats.first, this.page,
            this.cds[0].kind, this.drawHeatMap, true);
        const yBucketCount = HistogramViewBase.bucketCount(this.stats.second, this.page,
            this.cds[1].kind, this.drawHeatMap, false);
        const arg0 = HistogramViewBase.getRange(this.stats.first,
            this.cds[0], this.ds[0], xBucketCount);
        const arg1 = HistogramViewBase.getRange(this.stats.second,
            this.cds[1], this.ds[1], yBucketCount);
        let samplingRate: number;
        if (this.drawHeatMap) {
            // We cannot sample when we need to distinguish reliably 1 from 0.
            samplingRate = 1.0;
        } else {
            samplingRate = HistogramViewBase.samplingRate(xBucketCount, this.stats.first.presentCount, this.page);
        }
        if (this.exact) {
            samplingRate = 1.0;
        }

        const size = PlottingSurface.getDefaultChartSize(this.page);
        const cdfCount = Math.floor(size.width);

        const arg: Histogram2DArgs = {
            first: arg0,
            second: arg1,
            samplingRate,
            seed: samplingRate >= 1.0 ? 0 : Seed.instance.get(),
            xBucketCount,
            yBucketCount,
            cdfBucketCount: cdfCount,
            cdfSamplingRate: HistogramViewBase.samplingRate(cdfCount, this.stats.first.presentCount, this.page),
        };
        if (this.drawHeatMap) {
            const rr = this.remoteObject.createHeatMapRequest(arg);
            const renderer = new HeatMapRenderer(this.page,
                this.remoteObject, this.rowCount, this.schema,
                this.cds, [this.stats.first, this.stats.second],
                samplingRate, [this.ds[0], this.ds[1]], rr, this.reusePage);
            if (this.operation != null) {
                rr.setStartTime(this.operation.startTime());
            }
            rr.invoke(renderer);
        } else {
            const rr = this.remoteObject.createHistogram2DRequest(arg);
            const renderer = new Histogram2DRenderer(this.page,
                this.remoteObject, this.rowCount, this.schema,
                this.cds, [this.stats.first, this.stats.second], samplingRate,
                this.ds, rr, this.relative, this.reusePage);
            if (this.operation != null) {
                rr.setStartTime(this.operation.startTime());
            }
            rr.invoke(renderer);
        }
    }

    public onCompleted(): void {
        super.onCompleted();
        if (this.stats == null) {
            // probably some error occurred
            return;
        }
        this.draw();
    }
}

/**
 * Renders a heatmap
 */
export class HeatMapRenderer extends Receiver<HeatMap> {
    protected heatMap: HeatmapView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected cds: IColumnDescription[],
                protected stats: BasicColStats[],
                protected samplingRate: number,
                protected ds: DistinctStrings[],
                operation: ICancellable,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Heatmap (" + schema.displayName(cds[0].name) + ", " +
                schema.displayName(cds[1].name) + ")", page),
            operation, "histogram");
        this.heatMap = new HeatmapView(
            remoteTable.remoteObjectId, rowCount, schema, this.page);
        this.page.setDataView(this.heatMap);
        if (cds.length !== 2) {
            throw new Error("Expected 2 columns, got " + cds.length);
        }
    }

    public onNext(value: PartialResult<HeatMap>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        const points = value.data.buckets;
        let xPoints = 1;
        let yPoints = 1;
        if (points != null) {
            xPoints = points.length;
            yPoints = points[0] != null ? points[0].length : 1;
        }

        const xAxisData = new AxisData(this.cds[0], this.stats[0], this.ds[0], xPoints);
        const yAxisData = new AxisData(this.cds[1], this.stats[1], this.ds[1], yPoints);
        this.heatMap.updateView(value.data, xAxisData, yAxisData,
            this.samplingRate, false, this.elapsedMilliseconds());
    }
}

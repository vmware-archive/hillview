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

import {d3} from "../ui/d3-modules";

import { Renderer } from "../rpc";
import {
    ColumnDescription, Schema, RecordOrder, BasicColStats, FilterDescription,
    Histogram2DArgs, CombineOperators, RemoteObjectId, HeatMap
} from "../javaBridge";
import {TableView, NextKReceiver} from "./tableView";
import {
    Pair, significantDigits, formatNumber, reorder, ICancellable, PartialResult, Seed
} from "../util";
import {HistogramViewBase} from "./histogramViewBase";
import {TopMenu, SubMenu} from "../ui/menu";
import {Histogram2DRenderer, Make2DHistogram, Filter2DReceiver} from "./histogram2DView";
import {Point, Resolution} from "../ui/ui";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {TextOverlay} from "../ui/textOverlay";
import {AxisData} from "./axisData";
import {RemoteTableObjectView, ZipReceiver, RemoteTableObject} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";
import {PlottingSurface} from "../ui/plottingSurface";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {HistogramPlot} from "../ui/histogramPlot";

/**
 * A HeatMapView renders information as a heatmap.
 */
export class HeatMapView extends RemoteTableObjectView {
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
    private menu: TopMenu;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId,
                protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, originalTableId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.dragging = false;
        this.moved = false;
        this.menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view." },
                { text: "swap axes",
                    action: () => { this.swapAxes(); },
                    help: "Draw the heatmap with the same data by swapping the X and Y axes." },
                { text: "table",
                    action: () => { this.showTable(); },
                    help: "View the data underlying this view as a table."
                },
                { text: "histogram",
                    action: () => { this.histogram(); },
                    help: "Show this data as a two-dimensional histogram." },
            ]) },
            {
                text: "Combine", help: "Combine data in two separate views.", subMenu: combineMenu(this, page.pageId)
            }
        ]);

        this.page.setMenu(this.menu);
        this.topLevel.tabIndex = 1;

        let legendSurface = new PlottingSurface(this.topLevel, page);
        //legendSurface.setMargins(0, 0, 0, 0);
        legendSurface.setHeight(Resolution.legendSpaceHeight * 2/3);
        this.colorLegend = new HeatmapLegendPlot(legendSurface);
        this.colorLegend.setColorMapChangeEventListener(() => this.plot.reapplyColorMap());

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
                      samplingRate: number, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
        this.colorLegend.clear();
        this.plot.clear();
        if (this.showMissingData)
            this.xHistoPlot.clear();
        if (heatmap == null || heatmap.buckets.length == 0) {
            this.page.reportError("No data to display");
            return;
        }

        let xPoints = heatmap.buckets.length;
        let yPoints = heatmap.buckets[0].length;
        if (yPoints == 0) {
            this.page.reportError("No data to display");
            return;
        }

        this.currentData = {
            heatMap: heatmap,
            xData: xData,
            yData: yData,
            xPoints: xPoints,
            yPoints: yPoints,
            samplingRate: samplingRate
        };

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        let canvas = this.surface.getCanvas();
        canvas.call(drag)
            .on("mousemove", () => this.onMouseMove())
            .on("mouseenter", () => this.onMouseEnter())
            .on("mouseleave", () => this.onMouseLeave());

        // The order of these operations is important
        this.plot.setData(heatmap, xData, yData, samplingRate);
        this.colorLegend.setData(1, this.plot.getMaxCount());
        this.colorLegend.draw();
        this.plot.draw();
        let margin = this.plot.labelWidth();
        if (margin > this.surface.leftMargin) {
            this.surface.setMargins(null, null, null, margin);
            this.surface.moveCanvas();
        }
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
            [xData.description.name, yData.description.name, "count"], 40);
        this.pointDescription.show(false);
        let summary = formatNumber(this.plot.getVisiblePoints()) + " data points";
        if (heatmap.missingData != 0)
            summary += ", " + formatNumber(heatmap.missingData) + " missing";
        if (heatmap.histogramMissingX.missingData != 0)
            summary += ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate";
        if (heatmap.histogramMissingY.missingData != 0)
            summary += ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate";
        summary += ", " + formatNumber(this.plot.getDistinct()) + " distinct dots";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
    }


    histogram(): void {
        // Draw this as a 2-D histogram
        let rcol = new Range2DCollector([this.currentData.xData.description, this.currentData.yData.description],
                    this.tableSchema, [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
                    this.page, this, this.currentData.samplingRate >= 1, null, false);
        rcol.setValue({ first: this.currentData.xData.stats, second: this.currentData.yData.stats });
        rcol.onCompleted();
    }

    // combine two views according to some operation
    combine(how: CombineOperators): void {
        let r = SelectedObject.instance.getSelected(this, this.page.getErrorReporter());
        if (r == null)
            return;

        let rr = this.createZipRequest(r);
        let renderer = (page: FullPage, operation: ICancellable) => {
            return new Make2DHistogram(
                page, operation,
                [this.currentData.xData.description, this.currentData.yData.description],
                [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
                this.tableSchema, this.currentData.samplingRate >= 1, true, this.originalTableId);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.originalTableId, renderer));
    }

    // show the table corresponding to the data in the heatmap
    showTable(): void {
        let order =  new RecordOrder([ {
            columnDescription: this.currentData.xData.description,
            isAscending: true
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true
        }]);
        let page = new FullPage("Table view ", "Table", this.page);
        let table = new TableView(this.remoteObjectId, this.originalTableId, page);
        page.setDataView(table);
        table.setSchema(this.tableSchema);
        let rr = table.createNextKRequest(order, null);
        this.page.insertAfterMe(page);
        rr.invoke(new NextKReceiver(page, table, rr, false, order));
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code == "Escape")
            this.cancelDrag();
    }

    protected cancelDrag() {
        this.dragging = false;
        this.moved = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    public swapAxes(): void {
        let collector = new Range2DCollector(
            [this.currentData.yData.description, this.currentData.xData.description],
            this.tableSchema,
            [this.currentData.yData.distinctStrings, this.currentData.xData.distinctStrings],
            this.page, this, this.currentData.samplingRate >= 1, null, true);
        collector.setValue( {
            first: this.currentData.yData.stats,
            second: this.currentData.xData.stats });
        collector.onCompleted();
    }

    /*
    public exactHistogram(): void {
        if (this.currentData == null)
            return;
        let rc = new Range2DCollector(
            [this.currentData.xData.description, this.currentData.yData.description],
            this.tableSchema,
            [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
            this.page, this, true, null, true);
        rc.setValue({ first: this.currentData.xData.stats,
            second: this.currentData.yData.stats });
        rc.onCompleted();
    }
    */

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.heatMap,
            this.currentData.xData,
            this.currentData.yData,
            this.currentData.samplingRate,
            0);
    }

    protected onMouseEnter(): void {
        if (this.pointDescription != null)
            this.pointDescription.show(true);
    }

    protected onMouseLeave(): void {
        if (this.pointDescription != null)
            this.pointDescription.show(false);
    }

    onMouseMove(): void {
        if (this.plot.xScale == null)
            // not yet setup
            return;

        let position = d3.mouse(this.surface.getChart().node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = HistogramViewBase.invert(mouseX, this.plot.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.distinctStrings);
        let ys = HistogramViewBase.invert(mouseY, this.plot.yScale,
            this.currentData.yData.description.kind, this.currentData.yData.distinctStrings);

        let value = this.plot.getCount(mouseX, mouseY);
        this.pointDescription.update([xs, ys, value.toString()], mouseX, mouseY);
    }

    dragStart(): void {
        this.dragging = true;
        this.moved = false;
        let position = d3.mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    dragMove(): void {
        this.onMouseMove();
        if (!this.dragging)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let oy = this.selectionOrigin.y;
        let position = d3.mouse(this.surface.getCanvas().node());
        let x = position[0];
        let y = position[1];
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

    dragEnd(): void {
        if (!this.dragging || !this.moved)
            return;
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        let position = d3.mouse(this.surface.getCanvas().node());
        let x = position[0];
        let y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
    }

    /**
     * Selection has been completed.  The mouse coordinates are within the canvas.
     */
    selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        xl -= PlottingSurface.leftMargin;
        xr -= PlottingSurface.leftMargin;
        yl -= PlottingSurface.topMargin;
        yr -= PlottingSurface.topMargin;

        if (this.plot.xScale == null || this.plot.yScale == null)
            return;

        let xMin = HistogramViewBase.invertToNumber(xl, this.plot.xScale, this.currentData.xData.description.kind);
        let xMax = HistogramViewBase.invertToNumber(xr, this.plot.xScale, this.currentData.xData.description.kind);
        let yMin = HistogramViewBase.invertToNumber(yl, this.plot.yScale, this.currentData.yData.description.kind);
        let yMax = HistogramViewBase.invertToNumber(yr, this.plot.yScale, this.currentData.yData.description.kind);
        [xMin, xMax] = reorder(xMin, xMax);
        [yMin, yMax] = reorder(yMin, yMax);

        let xBoundaries: string[] = null;
        let yBoundaries: string[] = null;
        if (this.currentData.xData.distinctStrings != null)
            xBoundaries = this.currentData.xData.distinctStrings.categoriesInRange(xMin, xMax, xMax - xMin);
        if (this.currentData.yData.distinctStrings != null)
            yBoundaries = this.currentData.yData.distinctStrings.categoriesInRange(yMin, yMax, yMax - yMin);
        let xRange : FilterDescription = {
            min: xMin,
            max: xMax,
            kind: this.currentData.xData.description.kind,
            columnName: this.currentData.xData.description.name,
            bucketBoundaries: xBoundaries,
            complement: d3.event.sourceEvent.ctrlKey
        };
        let yRange : FilterDescription = {
            min: yMin,
            max: yMax,
            kind: this.currentData.yData.description.kind,
            columnName: this.currentData.yData.description.name,
            bucketBoundaries: yBoundaries,
            complement: d3.event.sourceEvent.ctrlKey
        };
        let rr = this.createFilter2DRequest(xRange, yRange);
        let renderer = new Filter2DReceiver(
            this.currentData.xData.description, this.currentData.yData.description,
            this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings,
            this.tableSchema, this.page, this.currentData.samplingRate >= 1, rr, true, this.originalTableId);
        rr.invoke(renderer);
    }
}

/**
 * Waits for all column stats to be received and then initiates a heatmap or 2D histogram.
  */
export class Range2DCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    protected stats: Pair<BasicColStats, BasicColStats>;
    constructor(protected cds: ColumnDescription[],
                protected tableSchema: Schema,
                protected ds: DistinctStrings[],
                page: FullPage,
                protected remoteObject: RemoteTableObject,
                protected exact: boolean,
                operation: ICancellable,
                protected drawHeatMap: boolean  // true - heatMap, false - histogram
    ) {
        super(page, operation, "range2d");
    }

    public setValue(bcs: Pair<BasicColStats, BasicColStats>): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: RemoteTableObject) {
        this.remoteObject = ro;
    }

    onNext(value: PartialResult<Pair<BasicColStats, BasicColStats>>): void {
        super.onNext(value);
        this.setValue(value.data);

        HistogramViewBase.adjustStats(this.cds[0].kind, this.stats.first);
        HistogramViewBase.adjustStats(this.cds[1].kind, this.stats.second);
    }

    public draw(): void {
        let xBucketCount = HistogramViewBase.bucketCount(this.stats.first, this.page,
            this.cds[0].kind, this.drawHeatMap, true);
        let yBucketCount = HistogramViewBase.bucketCount(this.stats.second, this.page,
            this.cds[1].kind, this.drawHeatMap, false);
        let arg0 = HistogramViewBase.getRange(this.stats.first,
            this.cds[0], this.ds[0], xBucketCount);
        let arg1 = HistogramViewBase.getRange(this.stats.second,
            this.cds[1], this.ds[1], yBucketCount);
        let samplingRate: number;
        if (this.drawHeatMap) {
            // We cannot sample when we need to distinguish reliably 1 from 0.
            samplingRate = 1.0;
        } else {
            samplingRate = HistogramViewBase.samplingRate(xBucketCount, this.stats.first.presentCount, this.page);
        }
        if (this.exact)
            samplingRate = 1.0;

        let size = PlottingSurface.getDefaultChartSize(this.page);
        let cdfCount = Math.floor(size.width);

        let arg: Histogram2DArgs = {
            first: arg0,
            second: arg1,
            samplingRate: samplingRate,
            seed: samplingRate >= 1.0 ? 0 : Seed.instance.get(),
            xBucketCount: xBucketCount,
            yBucketCount: yBucketCount,
            cdfBucketCount: cdfCount,
            cdfSamplingRate: HistogramViewBase.samplingRate(cdfCount, this.stats.first.presentCount, this.page)
        };
        if (this.drawHeatMap) {
            let rr = this.remoteObject.createHeatMapRequest(arg);
            let renderer = new HeatMapRenderer(this.page,
                this.remoteObject, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second],
                samplingRate, [this.ds[0], this.ds[1]], rr);
            if (this.operation != null)
                rr.setStartTime(this.operation.startTime());
            rr.invoke(renderer);
        } else {
            let rr = this.remoteObject.createHistogram2DMapRequest(arg);
            let renderer = new Histogram2DRenderer(this.page,
                this.remoteObject, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second], samplingRate, this.ds, rr);
            if (this.operation != null)
                rr.setStartTime(this.operation.startTime());
            rr.invoke(renderer);
        }
    }

    onCompleted(): void {
        super.onCompleted();
        if (this.stats == null)
            // probably some error occurred
            return;
        this.draw();
    }
}

/**
 * Renders a heatmap
  */
export class HeatMapRenderer extends Renderer<HeatMap> {
    protected heatMap: HeatMapView;

    constructor(page: FullPage,
                remoteTable: RemoteTableObject,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                protected samplingRate: number,
                protected ds: DistinctStrings[],
                operation: ICancellable) {
        super(new FullPage("Heatmap " + cds[0].name + ", " + cds[1].name, "Heatmap", page), operation, "histogram");
        page.insertAfterMe(this.page);
        this.heatMap = new HeatMapView(remoteTable.remoteObjectId, remoteTable.originalTableId, schema, this.page);
        this.page.setDataView(this.heatMap);
        if (cds.length != 2)
            throw "Expected 2 columns, got " + cds.length;
    }

    onNext(value: PartialResult<HeatMap>): void {
        super.onNext(value);
        if (value == null)
            return;

        let points = value.data.buckets;
        let xPoints = 1;
        let yPoints = 1;
        if (points != null) {
            xPoints = points.length;
            yPoints = points[0] != null ? points[0].length : 1;
        }

        let xAxisData = new AxisData(this.cds[0], this.stats[0], this.ds[0], xPoints);
        let yAxisData = new AxisData(this.cds[1], this.stats[1], this.ds[1], yPoints);
        this.heatMap.updateView(value.data, xAxisData, yAxisData,
            this.samplingRate, this.elapsedMilliseconds());
    }
}

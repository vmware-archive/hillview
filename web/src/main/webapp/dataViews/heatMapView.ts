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
import {ScaleLinear, ScaleTime} from "d3-scale";

import { Renderer } from "../rpc";
import {
    ColumnDescription, Schema, RecordOrder, Histogram, BasicColStats, FilterDescription,
    Histogram2DArgs, CombineOperators, RemoteObjectId
} from "../javaBridge";
import {TableView, NextKReceiver} from "./tableView";
import {
    Pair, significantDigits, formatNumber, reorder, regression, ICancellable, PartialResult, Seed
} from "../util";
import {HistogramViewBase} from "./histogramViewBase";
import {TopMenu, SubMenu} from "../ui/menu";
import {Histogram2DRenderer, Make2DHistogram, Filter2DReceiver} from "./histogram2DView";
import {KeyCodes, Point, Resolution, Size} from "../ui/ui";
import {FullPage} from "../ui/fullPage";
import {ColorLegend, ColorMap} from "../ui/colorLegend";
import {TextOverlay} from "../ui/textOverlay";
import {AxisData} from "./axisData";
import {RemoteTableObjectView, ZipReceiver, RemoteTableObject} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";

/**
 * Maximum number of colors that we expect users can distinguish reliably.
 */
// noinspection JSUnusedLocalSymbols
const colorResolution: number = 20;

// counterpart of Java class 'HeatMap'
export class HeatMapData {
    buckets: number[][];
    missingData: number;
    histogramMissingD1: Histogram;
    histogramMissingD2: Histogram;
    totalsize: number;
}

/**
 * A HeatMapView renders information as a heatmap.
 */
export class HeatMapView extends RemoteTableObjectView {
    protected dragging: boolean;
    protected svg: any;
    private selectionOrigin: Point;
    private selectionRectangle: any;
    protected chartDiv: HTMLElement;
    protected colorLegend: ColorLegend;
    protected colorMap: ColorMap;
    protected summary: HTMLElement;
    private xScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    private yScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    protected chartSize: Size;
    protected pointWidth: number;
    protected pointHeight: number;
    private moved: boolean;
    protected pointDescription: TextOverlay;

    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        missingData: number;
        data: number[][];
        xPoints: number;
        yPoints: number;
        samplingRate: number;
    };
    private chart: any;  // these are in fact a d3.Selection<>, but I can't make them typecheck
    protected canvas: any;
    private menu: TopMenu;

    constructor(remoteObjectId: RemoteObjectId, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, page);
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

        this.colorMap = new ColorMap();
        this.colorLegend = new ColorLegend(this.colorMap);
        this.colorLegend.setColorMapChangeEventListener(() => {
            this.reapplyColorMap();
        });
        this.topLevel.appendChild(this.colorLegend.getHTMLRepresentation());

        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
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
        let r = SelectedObject.current.getSelected();
        if (r == null) {
            this.page.reportError("No view selected");
            return;
        }

        let rr = this.createZipRequest(r);
        let renderer = (page: FullPage, operation: ICancellable) => {
            return new Make2DHistogram(
                page, operation,
                [this.currentData.xData.description, this.currentData.yData.description],
                [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
                this.tableSchema, this.currentData.samplingRate >= 1, true);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, renderer));
    }

    // show the table corresponding to the data in the heatmap
    showTable(): void {
        let table = new TableView(this.remoteObjectId, this.page);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.currentData.xData.description,
            isAscending: true
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true
        }]);
        let rr = table.createNextKRequest(order, null);
        let page = new FullPage("Table view ", "Table", this.page);
        page.setDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new NextKReceiver(page, table, rr, false, order));
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.keyCode == KeyCodes.escape)
            this.cancelDrag();
    }

    protected cancelDrag() {
        this.dragging = false;
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
            this.currentData.data,
            this.currentData.xData,
            this.currentData.yData,
            this.currentData.missingData,
            this.currentData.samplingRate,
            0);
    }

    public updateView(data: number[][], xData: AxisData, yData: AxisData,
                      missingData: number, samplingRate: number, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
        if (data == null || data.length == 0) {
            this.page.reportError("No data to display");
            return;
        }
        if (samplingRate >= 1) {
            let submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }

        let xPoints = data.length;
        let yPoints = data[0].length;
        if (yPoints == 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.currentData = {
            data: data,
            xData: xData,
            yData: yData,
            missingData: missingData,
            xPoints: xPoints,
            yPoints: yPoints,
            samplingRate: samplingRate
        };

        let canvasSize = Resolution.getCanvasSize(this.page);
        this.chartSize = Resolution.getChartSize(this.page);
        this.pointWidth = this.chartSize.width / xPoints;
        this.pointHeight = this.chartSize.height / yPoints;

        if (this.canvas != null)
            this.canvas.remove();

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        // Everything is drawn on top of the canvas.
        // The canvas includes the margins
        this.canvas = d3.select(this.chartDiv)
            .append("svg")
            .attr("id", "canvas")
            .call(drag)
            .attr("width", canvasSize.width)
            .attr("border", 1)
            .attr("height", canvasSize.height)
            .attr("cursor", "crosshair");

        this.canvas.on("mousemove", () => this.onMouseMove());

        // The chart uses a fragment of the canvas offset by the margins
        this.chart = this.canvas
            .append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`);

        let xsc = this.currentData.xData.scaleAndAxis(this.chartSize.width, true, false);
        let ysc = this.currentData.yData.scaleAndAxis(this.chartSize.height, false, false);
        let xAxis = xsc.axis;
        let yAxis = ysc.axis;
        this.xScale = xsc.scale;
        this.yScale = ysc.scale;

        interface Dot {
            x: number,
            y: number,
            v: number
        }

        let dots: Dot[] = [];
        let max: number = 0;
        let visible: number = 0;
        let distinct: number = 0;
        for (let x = 0; x < data.length; x++) {
            for (let y = 0; y < data[x].length; y++) {
                let v = data[x][y];
                if (v > max)
                    max = v;
                if (v != 0) {
                    let rec = {
                        x: x * this.pointWidth,
                        y: this.chartSize.height - (y + 1) * this.pointHeight,  // +1 because it's the upper corner
                        v: v
                    };
                    visible += v;
                    distinct++;
                    dots.push(rec);
                }
            }
        }
        this.colorMap.min = 1;
        this.colorMap.max = max;
        if (max > ColorMap.logThreshold)
            this.colorMap.setLogScale(true);
        else
            this.colorMap.setLogScale(false);

        this.chart.selectAll()
            .data(dots)
            .enter()
            .append("g")
            .append("svg:rect")
            .attr("class", "heatMapCell")
            .attr("x", d => d.x)
            .attr("y", d => d.y)
            .attr("data-val", d => d.v)
            .attr("width", this.pointWidth)
            .attr("height", this.pointHeight)
            .style("stroke-width", 0)
            .style("fill", d => this.colorMap.apply(d.v));

        this.colorLegend.redraw();

        this.canvas.append("text")
            .text(yData.description.name)
            .attr("dominant-baseline", "text-before-edge");
        this.canvas.append("text")
            .text(xData.description.name)
            .attr("transform", `translate(${this.chartSize.width / 2}, 
                  ${this.chartSize.height + Resolution.topMargin + Resolution.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");

        this.chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", `translate(0, ${this.chartSize.height})`)
            .call(xAxis);

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);

        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);

        if (this.currentData.yData.description.kind != "Category") {
            // it makes no sense to do regressions for categorical values
            let regr = regression(data);
            if (regr.length == 2) {
                let b = regr[0];
                let a = regr[1];
                let y1 = this.chartSize.height - b * this.pointHeight;
                let y2 = this.chartSize.height - (a * data.length + b) * this.pointHeight;
                this.chart
                    .append("line")
                    .attr("x1", 0)
                    .attr("y1", y1)
                    .attr("x2", this.pointWidth * data.length)
                    .attr("y2", y2)
                    .attr("stroke", "black");
            }
        }

        this.pointDescription = new TextOverlay(this.chart, ["x", "y", "count"], 40);
        let summary = formatNumber(visible) + " data points";
        if (missingData != 0)
            summary += ", " + formatNumber(missingData) + " missing";
        if (xData.missing.missingData != 0)
            summary += ", " + formatNumber(xData.missing.missingData) + " missing Y coordinate";
        if (yData.missing.missingData != 0)
            summary += ", " + formatNumber(yData.missing.missingData) + " missing X coordinate";
        summary += ", " + formatNumber(distinct) + " distinct dots";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
    }

    private reapplyColorMap() {
        this.chart.selectAll(".heatMapCell")
            .datum(function() {return this.dataset;})
            .style("fill", d => this.colorMap.apply(d.val))
    }

    onMouseMove(): void {
        if (this.xScale == null)
            // not yet setup
            return;

        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = HistogramViewBase.invert(position[0], this.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.distinctStrings);
        let ys = HistogramViewBase.invert(position[1], this.yScale,
            this.currentData.yData.description.kind, this.currentData.yData.distinctStrings);

        let xi = position[0] / this.pointWidth;
        let yi = (this.chartSize.height - position[1]) / this.pointHeight;
        xi = Math.floor(xi);
        yi = Math.floor(yi);
        let value = "0";
        if (xi >= 0 && xi < this.currentData.xPoints &&
            yi >= 0 && yi < this.currentData.yPoints) {
            value = this.currentData.data[xi][yi].toString();
        }

        this.pointDescription.update([xs, ys, value], mouseX, mouseY);
    }

    dragStart(): void {
        if (this.chart == null)
            return;

        this.dragging = true;
        this.moved = false;
        let position = d3.mouse(this.chart.node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    dragMove(): void {
        if (this.chart == null)
            return;

        this.onMouseMove();
        if (!this.dragging)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let oy = this.selectionOrigin.y;
        let position = d3.mouse(this.chart.node());
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
            .attr("x", ox + Resolution.leftMargin)
            .attr("y", oy + Resolution.topMargin)
            .attr("width", width)
            .attr("height", height);
    }

    dragEnd(): void {
        if (this.chart == null)
            return;
        if (!this.dragging || !this.moved)
            return;
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        let position = d3.mouse(this.chart.node());
        let x = position[0];
        let y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
    }

    selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        if (this.xScale == null || this.yScale == null)
            return;

        let xMin = HistogramViewBase.invertToNumber(xl, this.xScale, this.currentData.xData.description.kind);
        let xMax = HistogramViewBase.invertToNumber(xr, this.xScale, this.currentData.xData.description.kind);
        let yMin = HistogramViewBase.invertToNumber(yl, this.yScale, this.currentData.yData.description.kind);
        let yMax = HistogramViewBase.invertToNumber(yr, this.yScale, this.currentData.yData.description.kind);
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
            this.tableSchema, this.page, this.currentData.samplingRate >= 1, rr, true);
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
            /* TODO: we need an accurate presentCount for both axes
            samplingRate = xBucketCount * yBucketCount * colorResolution * colorResolution /
                Math.min(this.stats.first.presentCount, this.stats.second.presentCount);
                */
            // We cannot sample when we need to distinguish reliably 1 from 0.
            samplingRate = 1.0;
        } else {
            samplingRate = HistogramViewBase.samplingRate(xBucketCount, this.stats.first.presentCount, this.page);
        }
        if (this.exact)
            samplingRate = 1.0;

        let arg: Histogram2DArgs = {
            first: arg0,
            second: arg1,
            samplingRate: samplingRate,
            seed: Seed.instance.get(),
            xBucketCount: xBucketCount,
            yBucketCount: yBucketCount
        };
        let rr = this.remoteObject.createHeatMapRequest(arg);
        if (this.operation != null)
            rr.setStartTime(this.operation.startTime());
        let renderer: Renderer<HeatMapData> = null;
        if (this.drawHeatMap) {
            renderer = new HeatMapRenderer(this.page,
                this.remoteObject.remoteObjectId, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second],
                samplingRate, [this.ds[0], this.ds[1]], rr);
        } else {
            renderer = new Histogram2DRenderer(this.page,
                this.remoteObject.remoteObjectId, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second], samplingRate, this.ds, rr);
        }
        rr.invoke(renderer);
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
export class HeatMapRenderer extends Renderer<HeatMapData> {
    protected heatMap: HeatMapView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                protected samplingRate: number,
                protected ds: DistinctStrings[],
                operation: ICancellable) {
        super(new FullPage("Heatmap", "Heatmap", page), operation, "histogram");
        page.insertAfterMe(this.page);
        this.heatMap = new HeatMapView(remoteTableId, schema, this.page);
        this.page.setDataView(this.heatMap);
        if (cds.length != 2)
            throw "Expected 2 columns, got " + cds.length;
    }

    onNext(value: PartialResult<HeatMapData>): void {
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

        let xAxisData = new AxisData(value.data.histogramMissingD1, this.cds[0], this.stats[0], this.ds[0], xPoints);
        let yAxisData = new AxisData(value.data.histogramMissingD2, this.cds[1], this.stats[1], this.ds[1], yPoints);
        this.heatMap.updateView(value.data.buckets, xAxisData, yAxisData,
            value.data.missingData, this.samplingRate, this.elapsedMilliseconds());
    }
}

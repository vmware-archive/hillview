/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import d3 = require('d3');
import {
    FullPage, Point, Size, KeyCodes, significantDigits, formatNumber, translateString, Resolution
} from "./ui";
import { Renderer, combineMenu, CombineOperators, SelectedObject } from "./rpc";
import {
    ColumnDescription, Schema, ContentsKind, RecordOrder, DistinctStrings,
    RemoteTableObjectView, RemoteTableObject, Histogram, BasicColStats, FilterDescription,
    ColumnAndRange, ZipReceiver
} from "./tableData";
import {TableView, TableRenderer} from "./table";
import {Pair, reorder, regression, ICancellable, PartialResult} from "./util";
import {AnyScale, HistogramViewBase, ScaleAndAxis} from "./histogramBase";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {TopMenu, TopSubMenu} from "./menu";
import {Histogram2DRenderer, Make2DHistogram, Filter2DReceiver} from "./histogram2d";
import {ColorMap, ColorLegend} from "./vis";

// counterpart of Java class 'HeatMap'
export class HeatMapData {
    buckets: number[][];
    missingData: number;
    histogramMissingD1: Histogram;
    histogramMissingD2: Histogram;
    totalsize: number;
}

export class AxisData {
    public constructor(public missing: Histogram,
                       public description: ColumnDescription,
                       public stats: BasicColStats,
                       public distinctStrings: DistinctStrings,    // used only for categorical histograms
                       public bucketCount: number)
    {}

    public scaleAndAxis(length: number, bottom: boolean): ScaleAndAxis {
        return HistogramViewBase.createScaleAndAxis(
            this.description.kind, this.bucketCount, length,
            this.stats.min, this.stats.max, this.distinctStrings, true, bottom);
    }
}

export class HeatMapView extends RemoteTableObjectView {
    protected dragging: boolean;
    protected svg: any;
    private selectionOrigin: Point;
    private selectionRectangle: d3.Selection<BaseType, any, BaseType, BaseType>;
    private xLabel: HTMLElement;
    private yLabel: HTMLElement;
    private valueLabel: HTMLElement;
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

    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        missingData: number;
        data: number[][];
        xPoints: number;
        yPoints: number;
    };
    private chart: any;  // these are in fact a d3.Selection<>, but I can't make them typecheck
    protected canvas: any;
    private xDot: any;
    private yDot: any;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.dragging = false;
        this.moved = false;
        let menu = new TopMenu( [
            { text: "View", subMenu: new TopSubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "swap axes", action: () => { this.swapAxes(); } },
                { text: "table", action: () => { this.showTable(); } },
            ]) },
            {
                text: "Combine", subMenu: combineMenu(this)
            }
        ]);

        this.topLevel.appendChild(menu.getHTMLRepresentation());
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

        let position = document.createElement("table");
        this.topLevel.appendChild(position);
        position.className = "noBorder";
        let body = position.createTBody();
        let row = body.insertRow();
        row.className = "noBorder";

        let infoWidth = "150px";
        let labelCell = row.insertCell(0);
        labelCell.width = infoWidth;
        this.xLabel = document.createElement("div");
        this.xLabel.style.textAlign = "left";
        labelCell.appendChild(this.xLabel);
        labelCell.className = "noBorder";

        labelCell = row.insertCell(1);
        labelCell.width = infoWidth;
        this.yLabel = document.createElement("div");
        this.yLabel.style.textAlign = "left";
        labelCell.appendChild(this.yLabel);
        labelCell.className = "noBorder";

        labelCell = row.insertCell(2);
        labelCell.width = infoWidth;
        this.valueLabel = document.createElement("div");
        this.valueLabel.style.textAlign = "left";
        labelCell.appendChild(this.valueLabel);
        labelCell.className = "noBorder";
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
                this.tableSchema, true);
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
        let page = new FullPage();
        page.setDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new TableRenderer(page, table, rr, false, order));
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
            this.page, this, null, true);
        collector.setValue( {
            first: this.currentData.yData.stats,
            second: this.currentData.xData.stats });
        collector.onCompleted();
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.data,
            this.currentData.xData,
            this.currentData.yData,
            this.currentData.missingData,
            0);
    }

    public updateView(data: number[][], xData: AxisData, yData: AxisData,
                      missingData: number, elapsedMs: number) : void {
        this.page.reportError("Operation took " + significantDigits(elapsedMs/1000) + " seconds");
        if (data == null || data.length == 0) {
            this.page.reportError("No data to display");
            return;
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
            yPoints: yPoints
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
            .attr("transform", translateString(Resolution.leftMargin, Resolution.topMargin));

        let xsc = this.currentData.xData.scaleAndAxis(this.chartSize.width, true);
        let ysc = this.currentData.yData.scaleAndAxis(this.chartSize.height, false);
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
            .attr("transform", translateString(
                this.chartSize.width / 2, this.chartSize.height + Resolution.topMargin + Resolution.bottomMargin / 2))
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");

        this.chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", translateString(0, this.chartSize.height))
            .call(xAxis);

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);

        let dotRadius = 3;
        this.xDot = this.chart
            .append("circle")
            .attr("r", dotRadius)
            .attr("cy", this.chartSize.height)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.yDot = this.chart
            .append("circle")
            .attr("r", dotRadius)
            .attr("cx", 0)
            .attr("cy", 0)
            .attr("fill", "blue");

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

        let summary = formatNumber(visible) + " data points";
        if (missingData != 0)
            summary += ", " + formatNumber(missingData) + " missing";
        if (xData.missing.missingData != 0)
            summary += ", " + formatNumber(xData.missing.missingData) + " missing Y coordinate";
        if (yData.missing.missingData != 0)
            summary += ", " + formatNumber(yData.missing.missingData) + " missing X coordinate";
        summary += ", " + formatNumber(distinct) + " distinct dots";
        this.summary.textContent = summary;
    }

    private reapplyColorMap() {
        this.chart.selectAll(".heatMapCell")
            .datum(function() {return this.dataset;})
            .style("fill", d => this.colorMap.apply(d.val))
    }

    static invert(v: number, scale: AnyScale, kind: ContentsKind, allStrings: DistinctStrings): string {
        let inv = scale.invert(v);
        if (kind == "Integer")
            inv = Math.round(<number>inv);
        let result = String(inv);
        if (kind == "Category")
            result = allStrings.get(<number>inv);
        else if (kind == "Integer" || kind == "Double")
            result = significantDigits(<number>inv);
        // For Date do nothing
        return result;
    }

    onMouseMove(): void {
        if (this.xScale == null)
            // not yet setup
            return;

        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = HeatMapView.invert(position[0], this.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.distinctStrings);
        let ys = HeatMapView.invert(position[1], this.yScale,
            this.currentData.yData.description.kind, this.currentData.yData.distinctStrings);

        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;

        let xi = position[0] / this.pointWidth;
        let yi = (this.chartSize.height - position[1]) / this.pointHeight;
        xi = Math.floor(xi);
        yi = Math.floor(yi);
        if (xi >= 0 && xi < this.currentData.xPoints &&
            yi >= 0 && yi < this.currentData.yPoints) {
            let v = this.currentData.data[xi][yi];
            this.valueLabel.textContent = "value=" + v;
        } else {
            this.valueLabel.textContent = "";
        }

        this.xDot.attr("cx", mouseX);
        this.yDot.attr("cy", mouseY);
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
            columnName: this.currentData.xData.description.name,
            bucketBoundaries: xBoundaries,
            complement: d3.event.sourceEvent.ctrlKey
        };
        let yRange : FilterDescription = {
            min: yMin,
            max: yMax,
            columnName: this.currentData.yData.description.name,
            bucketBoundaries: yBoundaries,
            complement: d3.event.sourceEvent.ctrlKey
        };
        let rr = this.createFilter2DRequest(xRange, yRange);
        let renderer = new Filter2DReceiver(
            this.currentData.xData.description, this.currentData.yData.description,
            this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings,
            this.tableSchema, this.page, rr, true);
        rr.invoke(renderer);
    }
}

// Waits for all column stats to be received and then initiates a heatmap or 2Dhistogram.
export class Range2DCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    protected stats: Pair<BasicColStats, BasicColStats>;
    constructor(protected cds: ColumnDescription[],
                protected tableSchema: Schema,
                protected ds: DistinctStrings[],
                page: FullPage,
                protected remoteObject: RemoteTableObject,
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
    }

    public draw(): void {
        let xBucketCount = HistogramViewBase.bucketCount(
            this.stats.first, this.page, this.cds[0].kind, this.drawHeatMap, true);
        let yBucketCount = HistogramViewBase.bucketCount(
            this.stats.second, this.page, this.cds[1].kind, this.drawHeatMap, false);

        let xBoundaries = this.ds != null && this.ds[0] != null ?
            this.ds[0].categoriesInRange(this.stats.first.min, this.stats.first.max, xBucketCount) : null;
        let yBoundaries = this.ds != null && this.ds[1] != null ?
            this.ds[1].categoriesInRange(this.stats.second.min, this.stats.second.max, yBucketCount) : null;
        let arg0: ColumnAndRange = {
            columnName: this.cds[0].name,
            min: this.stats.first.min,
            max: this.stats.first.max,
            samplingRate: 1.0,
            bucketCount: xBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: xBoundaries
        };
        let arg1: ColumnAndRange = {
            columnName: this.cds[1].name,
            min: this.stats.second.min,
            max: this.stats.second.max,
            samplingRate: 1.0,
            bucketCount: yBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: yBoundaries
        };
        let rr = this.remoteObject.createHeatMapRequest(arg0, arg1);
        if (this.operation != null)
            rr.setStartTime(this.operation.startTime());
        let renderer: Renderer<HeatMapData> = null;
        if (this.drawHeatMap) {
            renderer = new HeatMapRenderer(this.page,
                this.remoteObject.remoteObjectId, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second], [this.ds[0], this.ds[1]], rr);
        } else {
            renderer = new Histogram2DRenderer(this.page,
                this.remoteObject.remoteObjectId, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second], this.ds, rr);
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

// Renders a heatmap
export class HeatMapRenderer extends Renderer<HeatMapData> {
    protected heatMap: HeatMapView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                protected ds: DistinctStrings[],
                operation: ICancellable) {
        super(new FullPage(), operation, "histogram");
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
            value.data.missingData, this.elapsedMilliseconds());
        this.heatMap.scrollIntoView();
    }
}

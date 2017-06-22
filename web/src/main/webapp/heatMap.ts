/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {
    FullPage, Renderer, IHtmlElement, HillviewDataView, Point, Size, KeyCodes,
    significantDigits, formatNumber, translateString
} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult} from "./rpc";
import {ColumnDescription, Schema, ContentsKind, TableView, RecordOrder, TableRenderer, RangeInfo} from "./table";
import {Pair, Converters, reorder} from "./util";
import {BasicColStats, Histogram, ColumnAndRange, AnyScale, HistogramViewBase} from "./histogramBase";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {DropDownMenu, ContextMenu} from "./menu";
import {Histogram2DRenderer} from "./histogram2d";

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
                       public allStrings: string[])   // used only for categorical histograms
    {}

    public getAxis(length: number, bottom: boolean): [any, AnyScale] {
        // returns a pair scale/axis
        let scale: any = null;
        let resultScale: AnyScale = null;
        if (this.description.kind == "Double" ||
            this.description.kind == "Integer") {
            scale = d3.scaleLinear()
                .domain([this.stats.min, this.stats.max]);
            resultScale = scale;
        } else if (this.description.kind == "Category") {
            let ticks: number[] = [];
            let labels: string[] = [];
            for (let i = 0; i < length; i++) {
                let index = i * (this.stats.max - this.stats.min) / length;
                index = Math.round(index);
                ticks.push(index * length / (this.stats.max - this.stats.min));
                labels.push(this.allStrings[this.stats.min + index]);
            }

            scale = d3.scaleOrdinal()
                .domain(labels)
                .range(ticks);
            resultScale = d3.scaleLinear()
                .domain([this.stats.min, this.stats.max]);
            // cast needed probably because the d3 typings are incorrect
        } else if (this.description.kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(this.stats.min);
            let maxDate: Date = Converters.dateFromDouble(this.stats.max);
            scale = d3
                .scaleTime()
                .domain([minDate, maxDate]);
            resultScale = scale;
        }
        if (bottom) {
            scale.range([0, length]);
            return [d3.axisBottom(scale), resultScale];
        } else {
            scale.range([length, 0]);
            return [d3.axisLeft(scale), resultScale];
        }
    }
}

export class HeatMapView extends RemoteObject
implements IHtmlElement, HillviewDataView {
    private topLevel: HTMLElement;
    public static readonly minChartWidth = 200;  // pixels
    public static readonly chartHeight = 400;  // pixels
    public static readonly minDotSize = 3;  // pixels
    public static readonly margin = {
        top: 30,
        right: 30,
        bottom: 50,
        left: 40
    };
    protected page: FullPage;
    protected dragging: boolean;
    protected svg: any;
    private selectionOrigin: Point;
    private selectionRectangle: d3.Selection<BaseType, any, BaseType, BaseType>;
    private xLabel: HTMLElement;
    private yLabel: HTMLElement;
    private valueLabel: HTMLElement;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;
    private xScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    private yScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    protected chartResolution: Size;
    protected pointWidth: number;
    protected pointHeight: number;

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
    private logScale: boolean;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.dragging = false;
        this.logScale = false;
        this.setPage(page);
        let menu = new DropDownMenu( [
            { text: "View", subMenu: new ContextMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "swap axes", action: () => { this.swapAxes(); } },
                { text: "table", action: () => { this.showTable(); } },
                //{ text: "log/linear scale", action: () => { this.changeScale(); }}
            ]) }
        ]);

        this.topLevel.appendChild(menu.getHTMLRepresentation());
        this.topLevel.tabIndex = 1;

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

    changeScale(): void {
        this.logScale = !this.logScale;
        this.refresh();
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
        page.setHillviewDataView(table);
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

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    // Generates a string that encodes a call to the SVG translate method
    static translateString(x: number, y: number): string {
        return "translate(" + String(x) + ", " + String(y) + ")";
    }

    public swapAxes(): void {
        let collector = new Range2DCollector(
            [this.currentData.yData.description, this.currentData.xData.description],
            this.tableSchema, this.page, this, null, true);
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

        let width = this.page.getWidthInPixels();
        let canvasHeight = HistogramViewBase.chartHeight;

        let chartWidth = width - HeatMapView.margin.left - HeatMapView.margin.right;
        let chartHeight = canvasHeight - HeatMapView.margin.top - HeatMapView.margin.bottom;
        if (chartWidth < HeatMapView.minChartWidth)
            chartWidth = HeatMapView.minChartWidth;

        this.pointWidth = chartWidth / xPoints;
        this.pointHeight = chartHeight / yPoints;

        this.chartResolution = { width: chartWidth, height: chartHeight };
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
            .attr("width", width)
            .attr("border", 1)
            .attr("height", canvasHeight)
            .attr("cursor", "crosshair");

        this.canvas.on("mousemove", () => this.onMouseMove());

        // The chart uses a fragment of the canvas offset by the margins
        this.chart = this.canvas
            .append("g")
            .attr("transform", HeatMapView.translateString(
                HeatMapView.margin.left, HeatMapView.margin.top));

        let xAxis, yAxis;
        [xAxis, this.xScale] = this.currentData.xData.getAxis(chartWidth, true);
        [yAxis, this.yScale] = this.currentData.yData.getAxis(chartHeight, false);

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
                        y: chartHeight - (y + 1) * this.pointHeight,  // +1 because it's the upper corner
                        v: v
                    };
                    visible += v;
                    distinct++;
                    dots.push(rec);
                }
            }
        }

        this.logScale = max > 20;

        if (max <= 1) {
            max = 1;
        } else {
            let legendWidth = 300;
            let legendHeight = 15;
            let legendSvg = this.canvas
                .append("svg");

            let gradient = legendSvg.append('defs')
                .append('linearGradient')
                .attr('id', 'gradient')
                .attr('x1', '0%')
                .attr('y1', '0%')
                .attr('x2', '100%')
                .attr('y2', '0%')
                .attr('spreadMethod', 'pad');

            for (let i = 0; i <= 100; i += 4) {
                gradient.append("stop")
                    .attr("offset", i + "%")
                    .attr("stop-color", HeatMapView.colorMap(i / 100))
                    .attr("stop-opacity", 1)
            }

            legendSvg.append("rect")
                .attr("width", legendWidth)
                .attr("height", legendHeight)
                .style("fill", "url(#gradient)")
                .attr("x", (chartWidth - legendWidth) / 2)
                .attr("y", 0);

            // create a scale and axis for the legend
            let legendScale;
            if (this.logScale) {
                let base = (max > 10000) ? 10 : 2;
                legendScale = d3.scaleLog()
                    .base(base);
            } else {
                legendScale = d3.scaleLinear();
            }

            let tickCount = max > 10 ? 10 : max - 1;
            legendScale
                .domain([1, max])
                .range([0, legendWidth])
                .ticks(tickCount);

            let legendAxis = d3.axisBottom(legendScale);
            legendSvg.append("g")
                .attr("transform", translateString(
                    (chartWidth - legendWidth) / 2, legendHeight))
                .call(legendAxis);
        }

        this.canvas.append("text")
            .text(yData.description.name)
            .attr("dominant-baseline", "hanging");
        this.canvas.append("text")
            .text(xData.description.name)
            .attr("transform", translateString(
                chartWidth / 2, HistogramViewBase.chartHeight - HistogramViewBase.margin.bottom))
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");

        this.chart.selectAll()
            .data(dots)
            .enter()
            .append("g")
            .append("svg:rect")
            .attr("class", "heatMapCell")
            .attr("x", d => d.x)
            .attr("y", d => d.y)
            .attr("width", this.pointWidth)
            .attr("height", this.pointHeight)
            .style("stroke-width", 0)
            .style("fill", d => this.color(d.v, max));

        this.chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", translateString(0, chartHeight))
            .call(xAxis);

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);

        let dotRadius = 3;
        this.xDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cy", chartHeight + HeatMapView.margin.top)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.yDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cx", HeatMapView.margin.left)
            .attr("cy", 0)
            .attr("fill", "blue");

        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);

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

    static colorMap(d: number): string {
        return d3.interpolateWarm(d);
    }

    color(d: number, max: number): string {
        if (max == 1)
            return "black";
        if (d == 0)
            throw "Zero should not have a color";
        if (this.logScale)
            return HeatMapView.colorMap(Math.log(d) / Math.log(max));
        else
            return HeatMapView.colorMap((d - 1) / (max - 1));
    }

    static invert(v: number, scale: AnyScale, kind: ContentsKind, allStrings: string[]): string {
        let inv = scale.invert(v);
        if (kind == "Integer")
            inv = Math.round(<number>inv);
        let result = String(inv);
        if (kind == "Category") {
            let index = Math.round(<number>inv);
            if (index >= 0 && index < allStrings.length)
                result = allStrings[index];
            else
                result = "";
        }
        else if (kind == "Integer" || kind == "Double")
            result = significantDigits(<number>inv);
        // For Date do nothing
        return result;
    }

    onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = HeatMapView.invert(position[0], this.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.allStrings);
        let ys = HeatMapView.invert(position[1], this.yScale,
            this.currentData.yData.description.kind, this.currentData.yData.allStrings);

        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;

        let canvasHeight = HistogramViewBase.chartHeight;
        let chartHeight = canvasHeight - HeatMapView.margin.top - HeatMapView.margin.bottom;

        let xi = position[0] / this.pointWidth;
        let yi = (chartHeight - position[1]) / this.pointHeight;
        xi = Math.floor(xi);
        yi = Math.floor(yi);
        if (xi >= 0 && xi < this.currentData.xPoints &&
            yi >= 0 && yi < this.currentData.yPoints) {
            let v = this.currentData.data[xi][yi];
            this.valueLabel.textContent = "value=" + v;
        } else {
            this.valueLabel.textContent = "";
        }

        this.xDot.attr("cx", mouseX + HeatMapView.margin.left);
        this.yDot.attr("cy", mouseY + HeatMapView.margin.top);
    }

    dragStart(): void {
        this.dragging = true;
        let position = d3.mouse(this.chart.node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    dragMove(): void {
        this.onMouseMove();
        if (!this.dragging)
            return;
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
            .attr("x", ox + HeatMapView.margin.left)
            .attr("y", oy + HeatMapView.margin.top)
            .attr("width", width)
            .attr("height", height);
    }

    dragEnd(): void {
        if (!this.dragging)
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
        if (this.currentData.xData.allStrings != null) {
            // it's enough to just send the first and last element for filtering.
            xBoundaries = [this.currentData.xData.allStrings[Math.floor(xMin)],
                this.currentData.xData.allStrings[Math.ceil(xMax)]];
        }
        if (this.currentData.yData.allStrings != null) {
            // it's enough to just send the first and last element for filtering.
            yBoundaries = [this.currentData.yData.allStrings[Math.floor(yMin)],
                this.currentData.xData.allStrings[Math.ceil(yMax)]];
        }
        let xRange : ColumnAndRange = {
            min: xMin,
            max: xMax,
            cdfBucketCount: null,  // unused
            bucketCount: null,  // unused
            columnName: this.currentData.xData.description.name,
            bucketBoundaries: xBoundaries
        };
        let yRange : ColumnAndRange = {
            min: yMin,
            max: yMax,
            cdfBucketCount: null,  // unused
            bucketCount: null,  // unused
            columnName: this.currentData.yData.description.name,
            bucketBoundaries: yBoundaries
        };
        let rr = this.createRpcRequest("filter2DRange", { first: xRange, second: yRange });
        let renderer = new Filter2DReceiver(
            [this.currentData.xData.description, this.currentData.yData.description],
            this.tableSchema,
            this.page,
            this, rr);
        rr.invoke(renderer);
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

    public static getRenderingSize(page: FullPage): Size {
        let width = page.getWidthInPixels();
        width = width - HeatMapView.margin.left - HeatMapView.margin.right;
        let height = HeatMapView.chartHeight - HeatMapView.margin.top - HeatMapView.margin.bottom;
        return { width: width, height: height };
    }
}

// After filtering we obtain a handle to a new table
export class Filter2DReceiver extends Renderer<string> {
    private stub: RemoteObject;

    constructor(protected cds: ColumnDescription[],
                protected tableSchema: Schema,
                page: FullPage,
                protected remoteObject: RemoteObject,
                operation: ICancellable) {
        super(page, operation, "Filter");
    }

    public onNext(value: PartialResult<string>): void {
        super.onNext(value);
        if (value.data != null)
            this.stub = new RemoteObject(value.data);
    }

    public onCompleted(): void {
        this.finished();
        if (this.stub != null) {
            let first = new RangeInfo();
            first.columnName = this.cds[0].name;
            let second = new RangeInfo();
            second.columnName = this.cds[1].name;
            let cols: RangeInfo[] = [first, second];
            let rr = this.stub.createRpcRequest("range2D", cols);
            rr.invoke(new Range2DCollector(
                this.cds, this.tableSchema, this.page, this.stub, rr, true));
        }
    }
}

// Waits for all column stats to be received and then initiates a heatmap or 2Dhistogram.
export class Range2DCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    protected stats: Pair<BasicColStats, BasicColStats>;
    constructor(protected cds: ColumnDescription[],
                protected tableSchema: Schema,
                page: FullPage,
                protected remoteObject: RemoteObject,
                operation: ICancellable,
                protected drawHeatMap: boolean  // true - heatMap, false - histogram
    ) {
        super(page, operation, "range2d");
    }

    public setValue(bcs: Pair<BasicColStats, BasicColStats>): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: RemoteObject) {
        this.remoteObject = ro;
    }

    onNext(value: PartialResult<Pair<BasicColStats, BasicColStats>>): void {
        super.onNext(value);
        this.setValue(value.data);
    }

    public draw(): void {
        let size = HeatMapView.getRenderingSize(this.page);
        let xBucketCount: number;
        let yBucketCount: number;
        if (this.drawHeatMap) {
            xBucketCount = Math.floor(size.width / HeatMapView.minDotSize);
            yBucketCount = Math.floor(size.height / HeatMapView.minDotSize);
        } else {
            xBucketCount = HistogramViewBase.bucketCount(this.stats.first, this.page, this.cds[0].kind);
            yBucketCount = HistogramViewBase.bucketCount(this.stats.second, this.page, this.cds[1].kind);
        }
        let arg0: ColumnAndRange = {
            columnName: this.cds[0].name,
            min: this.stats.first.min,
            max: this.stats.first.max,
            bucketCount: xBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: null // TODO
        };
        let arg1: ColumnAndRange = {
            columnName: this.cds[1].name,
            min: this.stats.second.min,
            max: this.stats.second.max,
            bucketCount: yBucketCount,
            cdfBucketCount: 0,
            bucketBoundaries: null // TODO
        };
        let args = {
            first: arg0,
            second: arg1
        };

        let rr = this.remoteObject.createRpcRequest("heatMap", args);
        if (this.operation != null)
            rr.setStartTime(this.operation.startTime());
        let renderer: Renderer<HeatMapData> = null;
        if (this.drawHeatMap) {
            renderer = new HeatMapRenderer(this.page,
                this.remoteObject.remoteObjectId, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second], rr);
        } else {
            renderer = new Histogram2DRenderer(this.page,
                this.remoteObject.remoteObjectId, this.tableSchema,
                this.cds, [this.stats.first, this.stats.second], rr);
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
                operation: ICancellable) {
        super(new FullPage(), operation, "histogram");
        page.insertAfterMe(this.page);
        this.heatMap = new HeatMapView(remoteTableId, schema, this.page);
        this.page.setHillviewDataView(this.heatMap);
        if (cds.length != 2)
            throw "Expected 2 columns, got " + cds.length;
    }

    onNext(value: PartialResult<HeatMapData>): void {
        super.onNext(value);
        if (value == null)
            return;
        let xAxisData = new AxisData(value.data.histogramMissingD1, this.cds[0], this.stats[0], null);
        let yAxisData = new AxisData(value.data.histogramMissingD2, this.cds[1], this.stats[1], null);
        this.heatMap.updateView(value.data.buckets, xAxisData, yAxisData,
            value.data.missingData, this.elapsedMilliseconds());
    }
}
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
    IHtmlElement, HieroDataView, FullPage, Renderer, significantDigits,
    Point, Size, formatNumber, percent
} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult} from "./rpc";
import {ColumnDescription, TableRenderer, TableView, RecordOrder} from "./table";
import {histogram} from "d3-array";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {ContextMenu, DropDownMenu} from "./menu";
import {Converters, Pair} from "./util";

// same as a Java class
interface Bucket1D {
    minObject: any;
    maxObject: any;
    minValue:  number;
    maxValue:  number;
    count:     number;
}

// same as a Java class
interface Histogram1D {
    missingData: number;
    outOfRange:  number;
    buckets:     Bucket1D[];
}

// same as Java class
interface Histogram1DLight {
    buckets: number[]
    missingData: number;
    outOfRange: number;
}

// same as Java class
interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    minObject: any;
    maxObject: any;
    moments: Array<number>;
    presentCount: number;
    missingCount: number;
}

export class Histogram extends RemoteObject
    implements IHtmlElement, HieroDataView {
    public static readonly maxBucketCount: number = 40;
    public static readonly minBarWidth: number = 5;
    public static readonly minChartWidth = 200;  // pixels
    public static readonly minChartHeight = 100;  // picles
    public static readonly maxChartHeight = 400;

    private topLevel: HTMLElement;
    public static readonly margin = {
        top: 30,
        right: 30,
        bottom: 30,
        left: 40
    };
    protected page: FullPage;
    protected svg: any;
    private selectionOrigin: Point;
    private selectionRectangle: d3.Selection<BaseType, any, BaseType, BaseType>;
    private xLabel: HTMLElement;
    private yLabel: HTMLElement;
    private cdfLabel: HTMLElement;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;
    private xScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    private yScale: ScaleLinear<number, number>;
    protected chartResolution: Size;
    // When plotting integer values we increase the data range by .5 on the left and right.
    // The adjustment is the number of pixels on screen that we "waste".
    // I.e., the cdf plot will start adjustment/2 pixels from the chart left margin
    // and will end adjustment/2 pixels from the right margin.
    private adjustment: number = 0;

    protected currentData: {
        histogram: Histogram1D,
        cdf: Histogram1DLight,
        description: ColumnDescription,
        stats: BasicColStats
    };
    private chart: any;  // these are in fact a d3.Selection<>, but I can't make them typecheck
    protected canvas: any;
    private xDot: any;
    private yDot: any;
    private cdfDot: any;

    constructor(id: string, page: FullPage) {
        super(id);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.setPage(page);
        let menu = new DropDownMenu( [
            { text: "View", subMenu: new ContextMenu([
                { text: "home", action: () => { TableView.goHome(this.page); } },
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "table", action: () => this.showTable() },
                { text: "#buckets", action: () => this.chooseBuckets() },
                { text: "correlate", action: () => this.chooseSecondColumn() },
            ]) }
        ]);

        this.topLevel.appendChild(menu.getHTMLRepresentation());
        this.topLevel.appendChild(document.createElement("hr"));
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
        this.cdfLabel = document.createElement("div");
        this.cdfLabel.style.textAlign = "left";
        labelCell.appendChild(this.cdfLabel);
        labelCell.className = "noBorder";
    }

    chooseSecondColumn(): void { // TODO
    }

    chooseBuckets(): void {
        if (this.currentData == null)
            return;

        let buckets = window.prompt("Choose number of buckets (between 1 and "
            + Histogram.maxBucketCount + ")", "10");
        if (buckets == null)
            return;
        if (isNaN(+buckets))
            this.page.reportError(buckets + " is not a number");

        let ws = this.page.getSize();
        let rr = this.createRpcRequest("histogram", {
            columnName: this.currentData.description.name,
            min: this.currentData.stats.min,
            max: this.currentData.stats.max,
            bucketCount: +buckets,
            width: ws.width,
            height: ws.height
        });
        let renderer = new HistogramRenderer(
            this.page, this.remoteObjectId, this.currentData.description, this.currentData.stats, rr);
        rr.invoke(renderer);
    }

    // show the table corresponding to the data in the histogram
    showTable(): void {
        let table = new TableView(this.remoteObjectId, this.page);
        this.page.setHieroDataView(table);
        let rr = table.createRpcRequest("getSchema", null);
        // TODO: put at least this column in the order
        rr.invoke(new TableRenderer(this.page, table, rr, false, new RecordOrder([])));
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    // Generates a string that encodes a call to the SVG translate method
    static translateString(x: number, y: number): string {
        return "translate(" + String(x) + ", " + String(y) + ")";
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.cdf,
            this.currentData.histogram,
            this.currentData.description,
            this.currentData.stats);
    }

    public updateView(cdf: Histogram1DLight, h: Histogram1D,
                      cd: ColumnDescription, stats: BasicColStats) : void {
        this.currentData = { cdf: cdf, histogram: h, description: cd, stats: stats };

        let ws = this.page.getSize();
        let width = ws.width;
        let height = ws.height;

        let chartWidth = width - Histogram.margin.left - Histogram.margin.right;
        let chartHeight = height - Histogram.margin.top - Histogram.margin.bottom;
        if (chartWidth < Histogram.minChartWidth)
            chartWidth = Histogram.minChartWidth;
        if (chartHeight > Histogram.maxChartHeight)
            chartHeight = Histogram.maxChartHeight;
        if (chartHeight < Histogram.minChartHeight)
            chartHeight = Histogram.minChartHeight;

        this.chartResolution = { width: chartWidth, height: chartHeight };

        let counts = h.buckets.map(b => b.count);
        let max = d3.max(counts);

        // prefix sum for cdf
        let sum = 0;
        for (let i in cdf.buckets) {
            sum += cdf.buckets[i];
            cdf.buckets[i] = sum;
        }

        let cdfData: number[] = [];
        let point = 0;
        for (let i in cdf.buckets) {
            cdfData.push(point);
            point = cdf.buckets[i] * max / stats.presentCount;
            cdfData.push(point);
        }

        if (this.canvas != null)
            this.canvas.remove();

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragging())
            .on("end", () => this.dragEnd());

        // Everything is drawn on top of the canvas.
        // The canvas includes the margins
        let canvasHeight = chartHeight + Histogram.margin.top + Histogram.margin.bottom;
        this.canvas = d3.select(this.chartDiv)
            .append("svg")
            .attr("id", "canvas")
            .call(drag)
            .attr("width", width)
            .attr("border", 1)
            .attr("height", canvasHeight);

        this.canvas.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", width)
            .attr("height", canvasHeight)
            .attr("stroke", "black")
            .attr("fill", "none");

        this.canvas.on("mousemove", () => this.onMouseMove());

        // The chart uses a fragment of the canvas offset by the margins
        this.chart = this.canvas
            .append("g")
            .attr("transform", Histogram.translateString(
                Histogram.margin.left, Histogram.margin.top));

        this.yScale = d3.scaleLinear()
            .domain([0, max])
            .range([chartHeight, 0]);
        let yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let minRange = stats.min;
        let maxRange = stats.max;
        this.adjustment = 0;
        if (cd.kind == "Integer" || cd.kind == "Category" || stats.min >= stats.max) {
            minRange -= .5;
            maxRange += .5;
            this.adjustment = chartWidth / (maxRange - minRange);
        }
        if (cd.kind == "Integer" ||
            cd.kind == "Double") {
            this.xScale = d3.scaleLinear()
                .domain([minRange, maxRange])
                .range([0, chartWidth]);
        } else if (cd.kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(minRange);
            let maxDate: Date = Converters.dateFromDouble(maxRange);
            this.xScale = d3
                .scaleTime()
                .domain([minDate, maxDate])
                .range([0, chartWidth]);
        }
        let xAxis = d3.axisBottom(this.xScale);

        // force a tick on x axis for degenerate scales
        if (stats.min >= stats.max)
            xAxis.ticks(1);

        this.canvas.append("text")
            .text(cd.name)
            .attr("transform", Histogram.translateString(
                chartWidth / 2, Histogram.margin.top/2))
            .attr("text-anchor", "middle");

        // After resizing the line may not have the exact number of points
        // as the screen width.
        let cdfLine = d3.line<number>()
            .x((d, i) => {
                let index = Math.floor(i / 2); // two points for each data point, for a zig-zag
                return this.adjustment/2 + index * 2 * (chartWidth - this.adjustment) / cdfData.length;
            })
            .y(d => this.yScale(d));

        // draw CDF curve
        this.canvas.append("path")
            .attr("transform", Histogram.translateString(
                Histogram.margin.left, Histogram.margin.top))
            .datum(cdfData)
            .attr("stroke", "blue")
            .attr("d", cdfLine)
            .attr("fill", "none");

        let barWidth = chartWidth / counts.length;
        let bars = this.chart.selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => Histogram.translateString(i * barWidth, 0));

        bars.append("rect")
            .attr("y", d => this.yScale(d))
            .attr("fill", "grey")
            .attr("height", d => chartHeight - this.yScale(d))
            .attr("width", barWidth - 1);

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", barWidth / 2)
            .attr("y", d => this.yScale(d))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text(d => (d == 0) ? "" : significantDigits(d))
            .exit();

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        this.chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", Histogram.translateString(0, chartHeight))
            .call(xAxis);

        let dotRadius = 3;
        this.xDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cy", chartHeight + Histogram.margin.top)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.yDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cx", Histogram.margin.left)
            .attr("cy", 0)
            .attr("fill", "blue");
        this.cdfDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("fill", "blue");

        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);

        let summary = "";
        if (h.missingData != 0)
            summary = formatNumber(h.missingData) + " missing, ";
        summary += formatNumber(stats.presentCount + stats.missingCount) + " points";
        this.summary.textContent = summary;
        console.log(String(counts.length) + " data points");
    }

    onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];
        let x = this.xScale.invert(position[0]);
        // TODO: handle strings
        if (this.currentData.description.kind == "Integer")
            x = Math.round(<number>x);
        let xs = String(x);
        if (this.currentData.description.kind == "Integer" ||
            this.currentData.description.kind == "Double")
            xs = significantDigits(<number>x);
        let y = Math.round(this.yScale.invert(position[1]));
        let ys = significantDigits(y);
        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;

        this.xDot.attr("cx", mouseX + Histogram.margin.left);
        this.yDot.attr("cy", mouseY + Histogram.margin.top);

        // determine mouse position on cdf curve
        // we have to take into account the adjustment
        let cdfX = (mouseX - this.adjustment/2) * this.currentData.cdf.buckets.length /
            (this.chartResolution.width - this.adjustment);
        let pos = 0;
        if (cdfX < 0) {
            pos = 0;
        } else if (cdfX >= this.currentData.cdf.buckets.length) {
            pos = 1;
        } else {
            let cdfPosition = this.currentData.cdf.buckets[Math.round(cdfX)];
            pos = cdfPosition / this.currentData.stats.presentCount;
        }

        this.cdfDot.attr("cx", mouseX + Histogram.margin.left);
        this.cdfDot.attr("cy", (1 - pos) * this.chartResolution.height + Histogram.margin.top);
        let perc = percent(pos);
        this.cdfLabel.textContent = "cdf=" + perc;
    }

    dragStart(): void {
        let position = d3.mouse(this.canvas.node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    dragging(): void {
        let ox = this.selectionOrigin.x;
        let oy = this.selectionOrigin.y;
        let position = d3.mouse(this.canvas.node());
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

        this.onMouseMove();

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", oy)
            .attr("width", width)
            .attr("height", height);
    }

    dragEnd(): void {
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);

        let position = d3.mouse(this.canvas.node());
        let x = position[0];
        let y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, this.selectionOrigin.y, x, y);
    }

    selectionCompleted(xl: number, yl: number, xr: number, yr: number): void {
        let x0 = this.xScale.invert(xl - Histogram.margin.left);
        let x1 = this.xScale.invert(xr - Histogram.margin.left);
        // TODO: rectangle overlap
        let y0 = this.yScale.invert(yl - Histogram.margin.top);
        let y1 = this.yScale.invert(yr - Histogram.margin.top);

        let min: number = 0;
        let max: number = 0;
        if (this.currentData.description.kind == "Integer" ||
            this.currentData.description.kind == "Double") {
            min = <number>x0;
            max = <number>x1;
        } else if (this.currentData.description.kind == "Date") {
            min = Converters.doubleFromDate(<Date>x0);
            max = Converters.doubleFromDate(<Date>x1);
        } // TODO: handle more types

        let range = {
            min: Math.min(min, max),
            max: Math.max(min, max),
            columnName: this.currentData.description.name,
            width: this.chartResolution.width,
            height: this.chartResolution.height
        };
        let rr = this.createRpcRequest("filterRange", range);
        let filterReceiver = new FilterReceiver(this.currentData.description, this.page, rr);
        rr.invoke(filterReceiver);
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
}

class TableStub extends RemoteObject {
    constructor(remoteObjectId: string) {
        super(remoteObjectId);
    }
}

// After filtering we obtain a handle to a new table
export class FilterReceiver extends Renderer<string> {
    private stub: TableStub;

    constructor(protected columnDescription: ColumnDescription,
                page: FullPage,
                operation: ICancellable) {
        super(page, operation, "Filter");
    }

    public onNext(value: PartialResult<string>): void {
        super.onNext(value);
        if (value.data != null)
            this.stub = new TableStub(value.data);
    }

    public onCompleted(): void {
        this.finished();
        if (this.stub != null) {
            // initiate a histogram on the new table
            let rr = this.stub.createRpcRequest("range", this.columnDescription.name);
            rr.invoke(new RangeCollector(this.columnDescription, this.page, this.stub, rr));
        }
    }
}

// Waits for all column stats to be received and then initiates a histogram
// rendering.
export class RangeCollector extends Renderer<BasicColStats> {
    protected stats: BasicColStats;
    constructor(protected cd: ColumnDescription,
                page: FullPage,
                protected remoteObject: RemoteObject,
                operation: ICancellable) {
        super(page, operation, "histogram");
    }

    onNext(value: PartialResult<BasicColStats>): void {
        super.onNext(value);
        this.stats = value.data;
    }

    onCompleted(): void {
        super.onCompleted();
        if (this.stats == null)
            // probably some error occurred
            return;
        if (this.stats.presentCount == 0) {
            this.page.reportError("No data in range");
            return;
        }

        let bucketCount = Histogram.maxBucketCount;
        if (this.stats.min >= this.stats.max)
            bucketCount = 1;
        let size = this.page.getSize();
        let width = size.width - Histogram.margin.left - Histogram.margin.right;
        let height = size.height - Histogram.margin.top - Histogram.margin.bottom;
        if (width / Histogram.minBarWidth < bucketCount)
            bucketCount = width / Histogram.minBarWidth;
        if (this.cd.kind == "Integer" ||
            this.cd.kind == "Category") {
            bucketCount = Math.min(bucketCount, this.stats.max - this.stats.min + 1);
        }

        let rr = this.remoteObject.createRpcRequest("histogram", {
            columnName: this.cd.name,
            min: this.stats.min,
            max: this.stats.max,
            bucketCount: bucketCount,
            width: width,
            height: height
        });
        let renderer = new HistogramRenderer(
            this.page, this.remoteObject.remoteObjectId, this.cd, this.stats, rr);
        rr.invoke(renderer);
    }
}

// Renders a column histogram
export class HistogramRenderer extends Renderer<Pair<Histogram1DLight, Histogram1D>> {
    protected histogram: Histogram;

    constructor(page: FullPage,
                remoteTableId: string,
                protected cd: ColumnDescription,
                protected stats: BasicColStats,
                operation: ICancellable) {
        super(page, operation, "histogram");
        this.histogram = new Histogram(remoteTableId, page);
        page.setHieroDataView(this.histogram);
    }

    onNext(value: PartialResult<Pair<Histogram1DLight, Histogram1D>>): void {
        super.onNext(value);
        this.histogram.updateView(value.data.first, value.data.second, this.cd, this.stats);
    }
}
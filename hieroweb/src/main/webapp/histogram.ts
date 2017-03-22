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
    IHtmlElement, HieroDataView, FullPage, Renderer, removeAllChildren, significantDigits,
    getWindowSize, Point
} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult, RpcReceiver} from "./rpc";
import {ColumnDescription, ContentsKind} from "./table";
import {histogram} from "d3-array";
import {BaseType} from "d3-selection";
import {ScaleLinear} from "d3-scale";

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
interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    minObject: any;
    maxObject: any;
    moments: Array<number>;
    rowCount: number;
}

export class Histogram extends RemoteObject
    implements IHtmlElement, HieroDataView {
    private topLevel: HTMLElement;
    public readonly margin = {
        top: 30,
        right: 30,
        bottom: 30,
        left: 40
    };
    private barWidth = 10;
    private topSpace = 20;
    protected page: FullPage;
    protected svg: any;
    private maxHeight = 300;
    private selectionOrigin: Point;
    private selectionRectangle: d3.Selection<BaseType, any, BaseType, BaseType>;
    private xLabel: HTMLElement;
    private yLabel: HTMLElement;
    private xScale: ScaleLinear<number, number>;
    private yScale: ScaleLinear<number, number>;

    protected currentData: {
        histogram: Histogram1D,
        description: ColumnDescription,
        stats: BasicColStats
    };
    private chart: any;  // it is in fact a d3.Selection<>, but I can't make it typecheck

    constructor(id: string, page: FullPage) {
        super(id);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.setPage(page);
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    // Generates a string that encodes a call to the SVG translate method
    static translateString(x: number, y: number): string {
        return "translate(" + String(x) + ", " + String(y) + ")";
    }

    public refresh(): void {
        this.updateView(this.currentData.histogram,
            this.currentData.description,
            this.currentData.stats);
    }

    public updateView(h: Histogram1D, cd: ColumnDescription, stats: BasicColStats) : void {
        this.currentData = { histogram: h, description: cd, stats: stats };

        let ws = getWindowSize();
        let width = ws.width;
        let height = ws.height;
        if (height > this.maxHeight)
            height = this.maxHeight;

        let chartWidth = width - this.margin.left - this.margin.right;
        let chartHeight = height - this.margin.top - this.margin.bottom;

        let counts = h.buckets.map(b => b.count);
        let max = d3.max(counts);
        let min = d3.min(counts);
        removeAllChildren(this.topLevel);

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragging())
            .on("end", () => this.dragEnd());
        // Everything is drawn on top of the canvas.
        // The canvas includes the margins
        let canvas = d3.select(this.topLevel)
            .append("svg")
            .call(drag)
            .attr("width", width)
            .attr("height", height);

        canvas.on("mousemove", () => this.onMouseMove());

        this.selectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);

        // The chart uses a fragment of the canvas offset by the margins
        this.chart = canvas
            .append("g")
            .attr("transform", Histogram.translateString(this.margin.left, this.margin.top));

        this.yScale = d3.scaleLinear()
            .domain([0, max])
            .range([chartHeight, 0]);
        let yAxis = d3.axisLeft(this.yScale);

        this.xScale = d3.scaleLinear()
            .domain([stats.min, stats.max])
            .range([0, chartWidth]);
        let xAxis = d3.axisBottom(this.xScale);

        canvas.append("text")
            .text(cd.name)
            .attr("transform", Histogram.translateString(chartWidth / 2, this.margin.top/2))
            .attr("text-anchor", "middle");

        let barWidth = chartWidth / counts.length;
        let bars = this.chart.selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => Histogram.translateString(i * barWidth, 0));

        bars.append("rect")
            .attr("y", d => this.yScale(d))
            .attr("height", d => chartHeight - this.yScale(d))
            .attr("width", barWidth - 1);

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", barWidth / 2)
            .attr("y", d => this.yScale(d))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (max / 2) ? "-.25em" : ".75em")
            .attr("fill", d => d <= (max / 2) ? "black" : "white")
            .text(d => (d == 0) ? "" : significantDigits(d))
            .exit();

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        this.chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", Histogram.translateString(0, chartHeight))
            .call(xAxis);

        console.log(String(counts.length) + " data points");

        let infoBox = document.createElement("div");
        this.topLevel.appendChild(infoBox);
        if (h.missingData != 0)
            infoBox.textContent = String(h.missingData) + " missing, ";
        infoBox.textContent += String(stats.rowCount) + " points";

        let position = document.createElement("table");
        this.topLevel.appendChild(position);
        position.className = "noBorder";
        let body = position.createTBody();
        let row = body.insertRow();
        row.className = "noBorder";

        let infoWidth = "100px";
        let labelCell = row.insertCell(0);
        labelCell.width = infoWidth;
        this.xLabel = document.createElement("div");
        this.xLabel.className = "leftAlign";
        labelCell.appendChild(this.xLabel);
        labelCell.className = "noBorder";

        labelCell = row.insertCell(1);
        labelCell.width = infoWidth;
        this.yLabel = document.createElement("div");
        this.yLabel.className = "leftAlign";
        labelCell.appendChild(this.yLabel);
        labelCell.className = "noBorder";
    }

    onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let x = this.xScale.invert(position[0]);
        // TODO: handle strings
        if (this.currentData.description.kind == ContentsKind.Integer)
            x = Math.round(x);
        let xs = significantDigits(x);
        let y = Math.round(this.yScale.invert(position[1]));
        let ys = significantDigits(y);
        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;
    }

    dragStart(): void {
        this.selectionOrigin = {
            x: d3.event.x,
            y: d3.event.y };
    }

    dragging(): void {
        let x = this.selectionOrigin.x;
        let y = this.selectionOrigin.y;
        let width = d3.event.x - x;
        let height = d3.event.y - y;

        if (width < 0) {
            x = d3.event.x;
            width = -width;
        }
        if (height < 0) {
            y = d3.event.y;
            height = -height;
        }

        this.selectionRectangle
            .attr("x", x)
            .attr("y", y)
            .attr("width", width)
            .attr("height", height);
    }

    dragEnd(): void {
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);

        let x = this.selectionOrigin.x;
        let y = this.selectionOrigin.y;
        this.selectionCompleted(x, y, d3.event.x, d3.event.y);
    }

    selectionCompleted(xl: number, yl: number, xr: number, yr: number): void {
        let x0 = this.xScale.invert(xl);
        let x1 = this.xScale.invert(xr);
        let y0 = this.yScale.invert(yl);
        let y1 = this.yScale.invert(yr);
        if (y0 < 0 && y1 < 0) {
            // If the selection is below the x axis then we only use the
            // x range to perform the selection
            let range = {
                min: Math.min(x0, x1),
                max: Math.max(x0, x1),
                column: this.currentData.description.name
            };
            let rr = this.createRpcRequest("filterRange", range);
            let rangeCollector = new RangeCollector(this.currentData.description, this.page, this, rr);
            rr.invoke(rangeCollector);
        } else {
            // TODO: find overlapping boxes
        }
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
export class FilterReceiver extends RpcReceiver<string> {
    private stub: TableStub;

    constructor(protected description: ColumnDescription,
                protected page: FullPage,
                operation: ICancellable) {
        super(page.progressManager.newProgressBar(operation, "Filter"),
            page.getErrorReporter());
    }

    // we expect exactly one reply
    public onNext(value: string): void {
        this.stub = new TableStub(value);
    }

    public onCompleted(): void {
        this.finished();
        // Retrieve the histogram
        let rr = this.stub.createRpcRequest("range", this.description.name);
        rr.invoke(new RangeCollector(this.description, this.page, this.stub, rr));
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
        this.progressBar.setPosition(value.done);
        this.stats = value.data;
    }

    onCompleted(): void {
        super.onCompleted();
        if (this.stats == null)
            // probably some error occurred
            return;
        let rr = this.remoteObject.createRpcRequest("histogram", {
            columnName: this.cd.name,
            min: this.stats.min,
            max: this.stats.max
        });
        let renderer = new HistogramRenderer(
            this.page, this.remoteObject.remoteObjectId, this.cd, this.stats, rr);
        rr.invoke(renderer);
    }
}

// Renders a column histogram
export class HistogramRenderer extends Renderer<Histogram1D> {
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

    onNext(value: PartialResult<Histogram1D>): void {
        this.progressBar.setPosition(value.done);
        this.histogram.updateView(value.data, this.cd, this.stats);
    }
}
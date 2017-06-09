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
    FullPage, Renderer, IHtmlElement, HieroDataView, Point, Size, KeyCodes, significantDigits, formatNumber
} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult} from "./rpc";
import {ColumnDescription, Schema} from "./table";
import {Pair, Converters} from "./util";
import {BasicColStats, HistogramView, Histogram1DLight, ColumnAndRange} from "./histogram";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {DropDownMenu, ContextMenu} from "./menu";

// counterpart of Java class
class HeatMap {
    buckets: number[][];
    missingData: number;
    histogramMissingD1: Histogram1DLight;
    histogramMissingD2: Histogram1DLight;
    totalsize: number;
}

class AxisData {
    public constructor(public missing: Histogram1DLight,
                       public description: ColumnDescription,
                       public stats: BasicColStats,
                       public allStrings: string[])   // used only for categorical histograms
    {}

    public getAxis(length: number, bottom: boolean): any {
        let scale: any = null;
        if (this.description.kind == "Double" ||
            this.description.kind == "Integer") {
            scale = d3.scaleLinear()
                .domain([this.stats.min, this.stats.max]);
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
            let mouseScale = d3.scaleLinear()
                .domain([this.stats.min, this.stats.max]);
            // cast needed probably because the d3 typings are incorrect
        } else if (this.description.kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(this.stats.min);
            let maxDate: Date = Converters.dateFromDouble(this.stats.max);
            scale = d3
                .scaleTime()
                .domain([minDate, maxDate]);
        }
        if (bottom) {
            scale.range([0, length]);
            return d3.axisBottom(scale);
        } else {
            scale.range([length, 0]);
            return d3.axisLeft(scale);
        }
    }
}

export class HeatMapView extends RemoteObject
implements IHtmlElement, HieroDataView {
    private topLevel: HTMLElement;
    public static readonly minChartWidth = 200;  // pixels
    public static readonly chartHeight = 400;  // pixels
    public static readonly minDotSize = 3;  // pixels
    public static readonly margin = {
        top: 30,
        right: 30,
        bottom: 30,
        left: 40
    };
    protected page: FullPage;
    protected dragging: boolean;
    protected svg: any;
    private selectionOrigin: Point;
    private selectionRectangle: d3.Selection<BaseType, any, BaseType, BaseType>;
    private xLabel: HTMLElement;
    private yLabel: HTMLElement;
    private cdfLabel: HTMLElement;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;
    private xScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    private yScale: ScaleLinear<number, number> | ScaleTime<number, number>;
    protected chartResolution: Size;

    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        missingData: number;
        data: number[][];
    };
    private chart: any;  // these are in fact a d3.Selection<>, but I can't make them typecheck
    protected canvas: any;
    private xDot: any;
    private yDot: any;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.dragging = false;
        this.setPage(page);
        let menu = new DropDownMenu( [
            { text: "View", subMenu: new ContextMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "swap axes", action: () => { this.swapAxes(); } },
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
        this.cdfLabel = document.createElement("div");
        this.cdfLabel.style.textAlign = "left";
        labelCell.appendChild(this.cdfLabel);
        labelCell.className = "noBorder";
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
        // TODO
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

    private linspace(start: number, end: number, n: number): number[] {
        let out = [];
        let delta = (end - start) / (n - 1);

        let i = 0;
        while (i < (n - 1)) {
            out.push(start + (i * delta));
            i++;
        }

        out.push(end);
        return out;
    }

    public updateView(data: number[][], xData: AxisData, yData: AxisData,
                      missingData: number, elapsedMs: number) : void {
        this.currentData = {
            data: data,
            xData: xData,
            yData: yData,
            missingData: missingData
        };
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

        // if xPoints or yPoints is 1 the display should degenerate into a histogram...

        let width = this.page.getWidthInPixels();
        let height = HistogramView.chartHeight;

        let chartWidth = width - HeatMapView.margin.left - HeatMapView.margin.right;
        let chartHeight = height - HeatMapView.margin.top - HeatMapView.margin.bottom;
        if (chartWidth < HeatMapView.minChartWidth)
            chartWidth = HeatMapView.minChartWidth;

        let pointWidth = chartWidth / (xPoints - 1);
        let pointHeight = chartHeight / (yPoints - 1);

        this.chartResolution = { width: chartWidth, height: chartHeight };
        if (this.canvas != null)
            this.canvas.remove();

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        // Everything is drawn on top of the canvas.
        // The canvas includes the margins
        let canvasHeight = chartHeight + HeatMapView.margin.top + HeatMapView.margin.bottom;
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

        let xAxis = this.currentData.xData.getAxis(chartWidth, true);
        let yAxis = this.currentData.yData.getAxis(chartHeight, false);

        interface Dot {
            x: number,
            y: number,
            v: number
        }

        let dots: Dot[] = [];
        let max: number = 0;
        let visible: number = 0;
        let distinct: number = 0;
        for (let x = 0; x < data.length; x++)
            for (let y = 0; y < data[x].length; y++) {
                let v = data[x][y];
                if (v > max)
                    max = v;
                if (v != 0) {
                    let rec = {
                        x: (x - .5) * pointWidth,
                        y: chartHeight - (y + .5) * pointHeight,
                        v: v
                    };
                    visible += v;
                    distinct++;
                    dots.push(rec);
                }
            }
        if (max == 0) {
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

            for (let i = 0; i <= 100; i++) {
                gradient.append("stop")
                    .attr("offset", i + "%")
                    .attr("stop-color", d3.interpolatePlasma(i / 100))
                    .attr("stop-opacity", 1)
            }

            let legendRect = legendSvg.append("rect")
                .attr("width", legendWidth)
                .attr("height", legendHeight)
                .style("fill", "url(#gradient)")
                .attr("x", (chartWidth - legendWidth) / 2)
                .attr("y", 0);

            // create a scale and axis for the legend
            var legendScale = d3.scaleLinear()
                .domain([0, max])
                .range([0, legendWidth]);

            var legendAxis = d3.axisBottom(legendScale);
            legendSvg.append("g")
                .attr("transform", HistogramView.translateString(
                    (chartWidth - legendWidth) / 2, legendHeight))
                .call(legendAxis);
        }

        let row = this.chart.selectAll()
            .data(dots)
            .enter()
            .append("g")
            .append("svg:rect")
            .attr("class", "heatMapCell")
            .attr("x", d => d.x)
            .attr("y", d => d.y)
            .attr("width", pointWidth)
            .attr("height", pointHeight)
            .style("stroke-width", 0)
            .style("fill", d => d3.interpolatePlasma(d.v / max));

        this.chart.append("g")
            .attr("class", "x-axis")
            .attr("transform", HistogramView.translateString(0, chartHeight))
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

    onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        /*
         let x : number | Date = 0;
        if (this.xScale != null)
            x = this.xScale.invert(position[0]);

        if (this.currentData.description.kind == "Integer")
            x = Math.round(<number>x);
        let xs = String(x);
        if (this.currentData.description.kind == "Category") {
            let index = Math.round(<number>x);
            if (index >= 0 && index < this.currentData.allStrings.length)
                xs = this.currentData.allStrings[index];
            else
                xs = "";
        }
        else if (this.currentData.description.kind == "Integer" ||
            this.currentData.description.kind == "Double")
            xs = significantDigits(<number>x);
        let y = Math.round(this.yScale.invert(position[1]));
        let ys = significantDigits(y);
        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;
        */

        this.xDot.attr("cx", mouseX + HeatMapView.margin.left);
        this.yDot.attr("cy", mouseY + HeatMapView.margin.top);
    }

    dragStart(): void {
        this.dragging = true;
        let position = d3.mouse(this.canvas.node());
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

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", oy)
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
        let position = d3.mouse(this.canvas.node());
        let x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x);
    }

    selectionCompleted(xl: number, xr: number): void {
        // TODO
        /*
        if (this.xScale == null)
            return;

        let x0 = this.xScale.invert(xl - Histogram.margin.left);
        let x1 = this.xScale.invert(xr - Histogram.margin.left);

        // selection could be done in reverse
        if (x0 > x1) {
            let tmp = x0;
            x0 = x1;
            x1 = tmp;
        }

        let min: number = 0;
        let max: number = 0;
        if (this.currentData.description.kind == "Integer" ||
            this.currentData.description.kind == "Double" ||
            this.currentData.description.kind == "Category") {
            min = Math.ceil(<number>x0);
            max = Math.floor(<number>x1);
            if (min > max) {
                this.page.reportError("No data selected");
                return;
            }
        } else if (this.currentData.description.kind == "Date") {
            min = Converters.doubleFromDate(<Date>x0);
            max = Converters.doubleFromDate(<Date>x1);
        }

        let boundaries: string[] = null;
        if (this.currentData.allStrings != null)
        // it's enough to just send the first and last element for filtering.
            boundaries = [this.currentData.allStrings[min], this.currentData.allStrings[max]];
        let range = {
            min: min,
            max: max,
            columnName: this.currentData.description.name,
            cdfBucketCount: this.chartResolution.width,
            bucketBoundaries: boundaries
        };

        let rr = this.createRpcRequest("filterRange", range);
        let renderer = new FilterReceiver(
            this.currentData.description, this.tableSchema,
            this.currentData.allStrings, range, this.page, rr);
        rr.invoke(renderer);
        */
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

// Waits for all column stats to be received and then initiates a heatmap.
export class Range2DCollector extends Renderer<Pair<BasicColStats, BasicColStats>> {
    protected stats: Pair<BasicColStats, BasicColStats>;
    constructor(protected cds: ColumnDescription[],
                protected tableSchema: Schema,
                page: FullPage,
                protected remoteObject: RemoteObject,
                operation: ICancellable) {
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

    public heatmap(): void {
        let size = HeatMapView.getRenderingSize(this.page);
        let xBucketCount = Math.floor(size.width / HeatMapView.minDotSize);
        let yBucketCount = Math.floor(size.height / HeatMapView.minDotSize);
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
        rr.setStartTime(this.operation.startTime());
        let renderer = new HeatMapRenderer(this.page,
            this.remoteObject.remoteObjectId, this.tableSchema,
            this.cds, [this.stats.first, this.stats.second], rr);
        rr.invoke(renderer);
    }

    onCompleted(): void {
        super.onCompleted();
        if (this.stats == null)
        // probably some error occurred
            return;
        this.heatmap();
    }
}

// Renders a column histogram
export class HeatMapRenderer extends Renderer<HeatMap> {
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
        this.page.setHieroDataView(this.heatMap);
        if (cds.length != 2)
            throw "Expected 2 columns, got " + cds.length;
    }

    onNext(value: PartialResult<HeatMap>): void {
        super.onNext(value);
        if (value == null)
            return;
        let xAxisData = new AxisData(value.data.histogramMissingD1, this.cds[0], this.stats[0], null);
        let yAxisData = new AxisData(value.data.histogramMissingD2, this.cds[1], this.stats[1], null);
        this.heatMap.updateView(value.data.buckets, xAxisData, yAxisData,
            value.data.missingData, this.elapsedMilliseconds());
    }
}
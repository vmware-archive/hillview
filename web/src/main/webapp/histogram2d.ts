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

import {HistogramViewBase, ColumnAndRange, BasicColStats} from "./histogramBase";
import {Schema, TableView, RecordOrder, TableRenderer, ColumnDescription} from "./table";
import {FullPage, significantDigits, percent, formatNumber, translateString, Renderer} from "./ui";
import {DropDownMenu, ContextMenu} from "./menu";
import d3 = require('d3');
import {reorder, Converters} from "./util";
import {FilterReceiver} from "./histogram";
import {AxisData, HeatMapData} from "./heatMap";
import {ICancellable, PartialResult} from "./rpc";

interface Rect {
    x: number;
    y: number;
    index: number;
    height: number;
}

export class Histogram2DView extends HistogramViewBase {
    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        missingData: number;
        data: number[][];
        xPoints: number;
        yPoints: number;
    };
    protected normalized: boolean;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, tableSchema, page);
        let menu = new DropDownMenu( [
            { text: "View", subMenu: new ContextMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "table", action: () => this.showTable() },
                { text: "percent/value", action: () => { this.normalized = !this.normalized; this.refresh(); } },
            ]) }
        ]);

        this.normalized = false;
        this.topLevel.insertBefore(menu.getHTMLRepresentation(), this.topLevel.children[0]);
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

    protected onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let x : number | Date = 0;
        if (this.xScale != null)
            x = this.xScale.invert(position[0]);

        if (this.currentData.xData.description.kind == "Integer")
            x = Math.round(<number>x);
        let xs = String(x);
        if (this.currentData.xData.description.kind == "Category") {
            let index = Math.round(<number>x);
            if (index >= 0 && index < this.currentData.xData.allStrings.length)
                xs = this.currentData.xData.allStrings[index];
            else
                xs = "";
        }
        else if (this.currentData.xData.description.kind == "Integer" ||
            this.currentData.xData.description.kind == "Double")
            xs = significantDigits(<number>x);
        let y = Math.round(this.yScale.invert(position[1]));
        let ys = significantDigits(y);
        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;

        this.xDot.attr("cx", mouseX + HistogramViewBase.margin.left);
        this.yDot.attr("cy", mouseY + HistogramViewBase.margin.top);

        /*
        if (this.currentData.cdfSum != null) {
            // determine mouse position on cdf curve
            // we have to take into account the adjustment
            let cdfX = (mouseX - this.adjustment / 2) * this.currentData.cdfSum.length /
                (this.chartResolution.width - this.adjustment);
            let pos = 0;
            if (cdfX < 0) {
                pos = 0;
            } else if (cdfX >= this.currentData.cdfSum.length) {
                pos = 1;
            } else {
                let cdfPosition = this.currentData.cdfSum[Math.floor(cdfX)];
                pos = cdfPosition / this.currentData.stats.presentCount;
            }

            this.cdfDot.attr("cx", mouseX + HistogramViewBase.margin.left);
            this.cdfDot.attr("cy", (1 - pos) * this.chartResolution.height + HistogramViewBase.margin.top);
            let perc = percent(pos);
            this.cdfLabel.textContent = "cdf=" + perc;
        }
        */
    }

    public updateView(data: number[][], xData: AxisData, yData: AxisData,
                      missingData: number, elapsedMs: number) : void {
        this.page.reportError("Operation took " + significantDigits(elapsedMs/1000) + " seconds");
        if (data == null || data.length == 0) {
            this.page.reportError("No data to display");
            return;
        }
        let xPoints = data.length;
        let yRectangles = data[0].length;
        if (yRectangles == 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.currentData = {
            data: data,
            xData: xData,
            yData: yData,
            missingData: missingData,
            xPoints: xPoints,
            yPoints: yRectangles
        };

        let width = this.page.getWidthInPixels();
        // Everything is drawn on top of the canvas.
        // The canvas includes the margins

        let chartWidth = width - HistogramViewBase.margin.left - HistogramViewBase.margin.right;
        if (chartWidth < HistogramViewBase.minChartWidth)
            chartWidth = HistogramViewBase.minChartWidth;
        let chartHeight = HistogramViewBase.chartHeight;
        let canvasHeight = chartHeight + HistogramViewBase.margin.top + HistogramViewBase.margin.bottom;
        this.chartResolution = { width: chartWidth, height: HistogramViewBase.chartHeight };

        /*
        let counts = h.buckets;
        let bucketCount = counts.length;
        let max = d3.max(counts);

        // prefix sum for cdf
        let cdfData: number[] = [];
        if (cdf != null) {
            this.currentData.cdfSum = [];

            let sum = 0;
            for (let i in cdf.buckets) {
                sum += cdf.buckets[i];
                this.currentData.cdfSum.push(sum);
            }

            let point = 0;
            for (let i in this.currentData.cdfSum) {
                cdfData.push(point);
                point = this.currentData.cdfSum[i] * max / stats.presentCount;
                cdfData.push(point);
            }
        }
        */

        if (this.canvas != null)
            this.canvas.remove();

        let counts: number[] = [];

        let max: number = 0;
        let rects : Rect[] = [];
        let total = 0;
        for (let x = 0; x < data.length; x++) {
            let yTotal = 0;
            for (let y = 0; y < data[x].length; y++) {
                let v = data[x][y];
                total += v;
                if (v != 0) {
                    let rec: Rect = {
                        x: x,
                        y: yTotal,
                        index: y,
                        height: v
                    };
                    rects.push(rec);
                }
                yTotal += v;
            }
            if (yTotal > max)
                max = yTotal;
            counts.push(yTotal);
        }

        if (max <= 0) {
            this.page.reportError("No data");
            return;
        }

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

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
            .attr("transform", translateString(
                HistogramViewBase.margin.left, HistogramViewBase.margin.top));

        this.yScale = d3.scaleLinear()
            .range([HistogramViewBase.chartHeight, 0]);
        if (this.normalized)
            this.yScale.domain([0, 100]);
        else
            this.yScale.domain([0, max]);
        let yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let cd = xData.description;
        let bucketCount = xPoints;
        let minRange = xData.stats.min;
        let maxRange = xData.stats.max;
        this.adjustment = 0;
        if (cd.kind == "Integer" || cd.kind == "Category" || xData.stats.min >= xData.stats.max) {
            minRange -= .5;
            maxRange += .5;
            this.adjustment = chartWidth / (maxRange - minRange);
        }

        let xAxis = null;
        this.xScale = null;
        if (cd.kind == "Integer" ||
            cd.kind == "Double") {
            this.xScale = d3.scaleLinear()
                .domain([minRange, maxRange])
                .range([0, chartWidth]);
            xAxis = d3.axisBottom(this.xScale);
        } else if (cd.kind == "Category") {
            let ticks: number[] = [];
            let labels: string[] = [];
            for (let i = 0; i < bucketCount; i++) {
                let index = i * (maxRange - minRange) / bucketCount;
                index = Math.round(index);
                ticks.push(this.adjustment / 2 + index * chartWidth / (maxRange - minRange));
                labels.push(this.currentData.xData.allStrings[xData.stats.min + index]);
            }

            let axisScale = d3.scaleOrdinal()
                .domain(labels)
                .range(ticks);
            this.xScale = d3.scaleLinear()
                .domain([minRange, maxRange])
                .range([0, chartWidth]);
            xAxis = d3.axisBottom(<any>axisScale);
            // cast needed probably because the d3 typings are incorrect
        } else if (cd.kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(minRange);
            let maxDate: Date = Converters.dateFromDouble(maxRange);
            this.xScale = d3
                .scaleTime()
                .domain([minDate, maxDate])
                .range([0, chartWidth]);
            xAxis = d3.axisBottom(this.xScale);
        }

        // force a tick on x axis for degenerate scales
        if (xData.stats.min >= xData.stats.max && xAxis != null)
            xAxis.ticks(1);

        this.canvas.append("text")
            .text(xData.description.name)
            .attr("transform", translateString(chartWidth / 2, chartHeight +
                HistogramViewBase.margin.top + HistogramViewBase.margin.bottom / 2))
            .attr("text-anchor", "middle");
        this.canvas.append("text")
            .text(yData.description.name)
            .attr("transform", translateString(chartWidth / 2, 0))
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");

        /*
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
            .attr("transform", translateString(
                HistogramViewBase.margin.left, HistogramViewBase.margin.top))
            .datum(cdfData)
            .attr("stroke", "blue")
            .attr("d", cdfLine)
            .attr("fill", "none");
            */

        let barWidth = chartWidth / bucketCount;
        let scale = chartHeight / max;
        this.chart.selectAll("g")
            // bars
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", d => d.x * barWidth)
            .attr("y", d => this.rectPosition(d, counts, scale))
            .attr("height", d => this.rectHeight(d, counts, scale))
            .attr("width", barWidth - 1)
            .attr("fill", d => this.color(d.index, yRectangles - 1))
            .exit()
            // label bars
            .data(counts)
            .enter()
            .append("g")
            .append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", (c, i) => (i + .5) * barWidth)
            .attr("y", d => chartHeight - (d * scale))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text(d => (d == 0) ? "" : significantDigits(d))
            .exit();

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        if (xAxis != null) {
            this.chart.append("g")
                .attr("class", "x-axis")
                .attr("transform", translateString(0, HistogramViewBase.chartHeight))
                .call(xAxis);
        }

        let dotRadius = 3;
        this.xDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cy", HistogramViewBase.chartHeight + HistogramViewBase.margin.top)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.yDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cx", HistogramViewBase.margin.left)
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

        let legendWidth = 500;
        if (legendWidth > chartWidth)
            legendWidth = chartWidth;
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
                .attr("stop-color", Histogram2DView.colorMap(i / 100))
                .attr("stop-opacity", 1)
        }

        legendSvg.append("rect")
            .attr("width", legendWidth)
            .attr("height", legendHeight)
            .style("fill", "url(#gradient)")
            .attr("x", (chartWidth - legendWidth) / 2)
            .attr("y", HistogramViewBase.margin.top / 3);

        // create a scale and axis for the legend
        let legendScale = d3.scaleLinear();
        legendScale
            .domain([this.currentData.yData.stats.min, this.currentData.yData.stats.max])
            .range([0, legendWidth]);

        let legendAxis = d3.axisBottom(legendScale);
        legendSvg.append("g")
            .attr("transform", translateString(
                (chartWidth - legendWidth) / 2, legendHeight + HistogramViewBase.margin.top / 3))
            .call(legendAxis);

        let summary = formatNumber(total) + " data points";
        if (missingData != 0)
            summary += ", " + formatNumber(missingData) + " missing";
        if (xData.missing.missingData != 0)
            summary += ", " + formatNumber(xData.missing.missingData) + " missing Y coordinate";
        if (yData.missing.missingData != 0)
            summary += ", " + formatNumber(yData.missing.missingData) + " missing X coordinate";
        summary += ", " + String(bucketCount) + " buckets";
        this.summary.textContent = summary;
    }

    protected rectHeight(d: Rect, counts: number[], scale: number): number {
        if (this.normalized) {
            let c = counts[d.x];
            if (c <= 0)
                return 0;
            return HistogramViewBase.chartHeight * d.height / c;
        }
        return d.height * scale;
    }

    protected rectPosition(d: Rect, counts: number[], scale: number): number {
        let y = d.y + d.height;
        if (this.normalized) {
            let c = counts[d.x];
            if (c <= 0)
                return 0;
            return HistogramViewBase.chartHeight * (1 - y / c);
        }
        return HistogramViewBase.chartHeight - y * scale;
    }

    static colorMap(d: number): string {
        return d3.interpolateRainbow(d);
    }

    color(d: number, max: number): string {
        return Histogram2DView.colorMap(d / max);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        let table = new TableView(this.remoteObjectId, this.page);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.currentData.xData.description,
            isAscending: true
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true
        } ]);
        let rr = table.createNextKRequest(order, null);
        let page = new FullPage();
        page.setHillviewDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new TableRenderer(page, table, rr, false, order));
    }

    protected selectionCompleted(xl: number, xr: number): void {
        if (this.xScale == null)
            return;

        let kind = this.currentData.xData.description.kind;
        let x0 = HistogramViewBase.invertToNumber(xl, this.xScale, kind);
        let x1 = HistogramViewBase.invertToNumber(xr, this.xScale, kind);

        // selection could be done in reverse
        let min: number;
        let max: number;
        [min, max] = reorder(x0, x1);
        if (min > max) {
            this.page.reportError("No data selected");
            return;
        }

        let boundaries: string[] = null;
        if (this.currentData.xData.allStrings != null) {
            // it's enough to just send the first and last element for filtering.
            boundaries = [this.currentData.xData.allStrings[Math.ceil(min)]];
            if (Math.floor(max) != Math.ceil(min))
                boundaries.push(this.currentData.xData.allStrings[Math.floor(max)]);
        }
        let range: ColumnAndRange = {
            min: min,
            max: max,
            columnName: this.currentData.xData.description.name,
            cdfBucketCount: null,  // unused for this call
            bucketCount: null,  // unused for this call
            bucketBoundaries: boundaries
        };

        /*
        let rr = this.createRpcRequest("filterRange", range);
        let renderer = new FilterReceiver(
            this.currentData.xData.description, this.tableSchema,
            this.currentData.xData.allStrings, range, this.page, rr);
        rr.invoke(renderer);
        */
    }
}

export class Histogram2DRenderer extends Renderer<HeatMapData> {
    protected histogram: Histogram2DView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                operation: ICancellable) {
        super(new FullPage(), operation, "histogram");
        page.insertAfterMe(this.page);
        this.histogram = new Histogram2DView(remoteTableId, schema, this.page);
        this.page.setHillviewDataView(this.histogram);
        if (cds.length != 2)
            throw "Expected 2 columns, got " + cds.length;
    }

    onNext(value: PartialResult<HeatMapData>): void {
        super.onNext(value);
        if (value == null)
            return;
        let xAxisData = new AxisData(value.data.histogramMissingD1, this.cds[0], this.stats[0], null);
        let yAxisData = new AxisData(value.data.histogramMissingD2, this.cds[1], this.stats[1], null);
        this.histogram.updateView(value.data.buckets, xAxisData, yAxisData,
            value.data.missingData, this.elapsedMilliseconds());
    }
}
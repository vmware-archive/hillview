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
    FullPage, Renderer, significantDigits, formatNumber, percent, translateString
} from "./ui";
import d3 = require('d3');
import {RemoteObject, ICancellable, PartialResult} from "./rpc";
import {ColumnDescription, TableRenderer, TableView, RecordOrder, Schema} from "./table";
import {histogram} from "d3-array";
import {TopMenu, TopSubMenu} from "./menu";
import {Converters, Pair, reorder} from "./util";
import {Histogram, HistogramViewBase, BasicColStats, ColumnAndRange, FilterDescription, BucketDialog} from "./histogramBase";

export class HistogramView extends HistogramViewBase {
    protected currentData: {
        histogram: Histogram,
        cdf: Histogram,
        cdfSum: number[],  // prefix sum of cdf
        description: ColumnDescription,
        stats: BasicColStats,
        allStrings: string[]   // used only for categorical histograms
    };

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, tableSchema, page);
        let menu = new TopMenu( [
            { text: "View", subMenu: new TopSubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "table", action: () => this.showTable() },
                { text: "#buckets", action: () => this.chooseBuckets() },
                //{ text: "correlate", action: () => this.chooseSecondColumn() },
            ]) }
        ]);

        this.topLevel.insertBefore(menu.getHTMLRepresentation(), this.topLevel.children[0]);
    }

    chooseSecondColumn(): void { // TODO
    }

    changeBuckets(bucketCount: number): void {
        let cdfBucketCount = this.currentData.cdf.buckets.length;
        let boundaries = HistogramViewBase.categoriesInRange(
            this.currentData.stats, cdfBucketCount, this.currentData.allStrings);
        let info: ColumnAndRange = {
            columnName: this.currentData.description.name,
            min: this.currentData.stats.min,
            max: this.currentData.stats.max,
            bucketCount: +bucketCount,
            cdfBucketCount: cdfBucketCount,
            bucketBoundaries: boundaries
        };
        let rr = this.createRpcRequest("histogram", info);
        let renderer = new HistogramRenderer(this.page,
            this.remoteObjectId, this.tableSchema, this.currentData.description,
            this.currentData.stats, rr, this.currentData.allStrings);
        rr.invoke(renderer);
    }

    chooseBuckets(): void {
        if (this.currentData == null)
            return;

        let bucket_dialog = new BucketDialog();
        bucket_dialog.setAction(() => this.changeBuckets(bucket_dialog.getBucketCount()));
        bucket_dialog.show();
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.cdf,
            this.currentData.histogram,
            this.currentData.description,
            this.currentData.stats,
            this.currentData.allStrings,
            0);
    }

    protected onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

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

        this.xDot.attr("cx", mouseX + HistogramViewBase.margin.left);
        this.yDot.attr("cy", mouseY + HistogramViewBase.margin.top);

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
    }

    public updateView(cdf: Histogram, h: Histogram,
                      cd: ColumnDescription, stats: BasicColStats,
                      allStrings: string[], elapsedMs: number) : void {
        this.currentData = {
            cdf: cdf,
            cdfSum: null,
            histogram: h,
            description: cd,
            stats: stats,
            allStrings: allStrings };
        this.page.reportError("Operation took " + significantDigits(elapsedMs/1000) + " seconds");

        let width = this.page.getWidthInPixels();

        let chartWidth = width - HistogramViewBase.margin.left - HistogramViewBase.margin.right;
        if (chartWidth < HistogramViewBase.minChartWidth)
            chartWidth = HistogramViewBase.minChartWidth;
        this.chartResolution = { width: chartWidth, height: HistogramViewBase.chartHeight };

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

        if (this.canvas != null)
            this.canvas.remove();

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        // Everything is drawn on top of the canvas.
        // The canvas includes the margins
        let canvasHeight = HistogramViewBase.chartHeight +
            HistogramViewBase.margin.top + HistogramViewBase.margin.bottom;
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
            .domain([0, max])
            .range([HistogramViewBase.chartHeight, 0]);
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
                labels.push(this.currentData.allStrings[stats.min + index]);
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
        if (stats.min >= stats.max && xAxis != null)
            xAxis.ticks(1);

        this.canvas.append("text")
            .text(cd.name)
            .attr("transform", translateString(
                chartWidth / 2, HistogramViewBase.margin.top/2))
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
            .attr("transform", translateString(
                HistogramViewBase.margin.left, HistogramViewBase.margin.top))
            .datum(cdfData)
            .attr("stroke", "blue")
            .attr("d", cdfLine)
            .attr("fill", "none");

        let barWidth = chartWidth / bucketCount;
        let bars = this.chart.selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => translateString(i * barWidth, 0));

        bars.append("rect")
            .attr("y", d => this.yScale(d))
            .attr("fill", "grey")
            .attr("height", d => HistogramViewBase.chartHeight - this.yScale(d))
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

        let summary = "";
        if (h.missingData != 0)
            summary = formatNumber(h.missingData) + " missing, ";
        summary += formatNumber(stats.presentCount + stats.missingCount) + " points";
        if (this.currentData.allStrings != null)
            summary += ", " + (this.currentData.stats.max - this.currentData.stats.min + 1) + " distinct values";
        summary += ", " + String(bucketCount) + " buckets";
        this.summary.textContent = summary;
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        let newPage = new FullPage();
        let table = new TableView(this.remoteObjectId, newPage);
        newPage.setDataView(table);
        this.page.insertAfterMe(newPage);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.currentData.description,
            isAscending: true
        } ]);
        let rr = table.createNextKRequest(order, null);
        rr.invoke(new TableRenderer(newPage, table, rr, false, order));
    }

    protected selectionCompleted(xl: number, xr: number): void {
        if (this.xScale == null)
            return;

        let kind = this.currentData.description.kind;
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
        if (this.currentData.allStrings != null) {
            // it's enough to just send the first and last element for filtering.
            boundaries = [this.currentData.allStrings[Math.ceil(min)]];
            if (Math.floor(max) != Math.ceil(min))
                boundaries.push(this.currentData.allStrings[Math.floor(max)]);
        }
        let filter: FilterDescription = {
            min: min,
            max: max,
            columnName: this.currentData.description.name,
            complement: d3.event.sourceEvent.ctrlKey,
            bucketBoundaries: boundaries
        };

        let rr = this.createRpcRequest("filterRange", filter);
        let renderer = new FilterReceiver(
                this.currentData.description, this.tableSchema,
            this.currentData.allStrings, filter, this.page, rr);
        rr.invoke(renderer);
    }
}

// After filtering we obtain a handle to a new table
export class FilterReceiver extends Renderer<string> {
    private stub: RemoteObject;

    constructor(protected columnDescription: ColumnDescription,
                protected tableSchema: Schema,
                protected allStrings: string[],
                protected filter: FilterDescription,
                page: FullPage,
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
            let fv = null;
            let sv = null;
            let fi = null;
            let li = null;
            if (this.filter.bucketBoundaries != null) {
                fv = this.filter.bucketBoundaries[0];
                sv = this.filter.bucketBoundaries[1];
                fi = this.filter.min;
                li = this.filter.max;
            }
            let rangeInfo = {
                columnName: this.columnDescription.name,
                firstIndex: fi,
                lastIndex: li,
                firstValue: fv,
                lastValue: sv
            };
            let rr = this.stub.createRpcRequest("range", rangeInfo);
            rr.invoke(new RangeCollector(this.columnDescription, this.tableSchema,
                this.allStrings, this.page, this.stub, rr));
        }
    }
}

// Waits for all column stats to be received and then initiates a histogram
// rendering.
export class RangeCollector extends Renderer<BasicColStats> {
    protected stats: BasicColStats;
    constructor(protected cd: ColumnDescription,
                protected tableSchema: Schema,
                protected allStrings: string[],  // for categorical columns only
                page: FullPage,
                protected remoteObject: RemoteObject,
                operation: ICancellable) {
        super(page, operation, "histogram");
    }

    public setValue(bcs: BasicColStats): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: RemoteObject) {
        this.remoteObject = ro;
    }

    onNext(value: PartialResult<BasicColStats>): void {
        super.onNext(value);
        this.setValue(value.data);
    }

    public histogram(): void {
        let size = HistogramViewBase.getRenderingSize(this.page);
        let bucketCount = HistogramViewBase.bucketCount(this.stats, this.page, this.cd.kind);
        let cdfCount = size.width;
        let boundaries = HistogramViewBase.categoriesInRange(this.stats, cdfCount, this.allStrings);
        let info: ColumnAndRange = {
            columnName: this.cd.name,
            min: this.stats.min,
            max: this.stats.max,
            bucketCount: bucketCount,
            cdfBucketCount: cdfCount,
            bucketBoundaries: boundaries
        };
        let rr = this.remoteObject.createRpcRequest("histogram", info);
        rr.setStartTime(this.operation.startTime());
        let renderer = new HistogramRenderer(this.page,
            this.remoteObject.remoteObjectId, this.tableSchema,
            this.cd, this.stats, rr, this.allStrings);
        rr.invoke(renderer);
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
        this.histogram();
    }
}

// Renders a column histogram
export class HistogramRenderer extends Renderer<Pair<Histogram, Histogram>> {
    protected histogram: HistogramView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cd: ColumnDescription,
                protected stats: BasicColStats,
                operation: ICancellable,
                protected allStrings: string[]) {
        super(new FullPage(), operation, "histogram");
        page.insertAfterMe(this.page);
        this.histogram = new HistogramView(remoteTableId, schema, this.page);
        this.page.setDataView(this.histogram);
    }

    onNext(value: PartialResult<Pair<Histogram, Histogram>>): void {
        super.onNext(value);
        this.histogram.updateView(value.data.first, value.data.second, this.cd,
            this.stats, this.allStrings, this.elapsedMilliseconds());
    }
}
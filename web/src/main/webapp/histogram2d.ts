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

import d3 = require('d3');
import { HistogramViewBase, BucketDialog, AnyScale } from "./histogramBase";
import {
    ColumnDescription, Schema, RecordOrder, ColumnAndRange, FilterDescription,
    ZipReceiver, RemoteTableRenderer, BasicColStats, DistinctStrings, RangeInfo
} from "./tableData";
import {TableView, TableRenderer} from "./table";
import {FullPage, significantDigits, formatNumber, translateString, Resolution, Rectangle} from "./ui";
import {TopMenu, TopSubMenu} from "./menu";
import {reorder, transpose, ICancellable, PartialResult} from "./util";
import {AxisData, HeatMapData, Range2DCollector} from "./heatMap";
import {combineMenu, CombineOperators, SelectedObject, Renderer} from "./rpc";

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
        visiblePoints: number;
    };
    protected normalized: boolean;
    protected selectingLegend: boolean;
    protected legendRect: Rectangle;  // legend position on the screen
    protected legend: any;  // a d3 object
    protected legendScale: AnyScale;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, tableSchema, page);
        let menu = new TopMenu( [
            { text: "View", subMenu: new TopSubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "table", action: () => this.showTable() },
                { text: "#buckets", action: () => this.chooseBuckets() },
                { text: "swap axes", action: () => { this.swapAxes(); } },
                { text: "heatmap", action: () => { this.heatmap(); } },
                { text: "percent/value", action: () => { this.normalized = !this.normalized; this.refresh(); } },
            ]) },
            {
                text: "Combine", subMenu: combineMenu(this)
            }
        ]);

        this.normalized = false;
        this.selectingLegend = true;
        this.topLevel.insertBefore(menu.getHTMLRepresentation(), this.topLevel.children[0]);
    }

    heatmap(): void {
        let rcol = new Range2DCollector([this.currentData.xData.description, this.currentData.yData.description],
            this.tableSchema, [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
            this.page, this, null, true);
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
                this.tableSchema, false);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, renderer));
    }

    public swapAxes(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            transpose(this.currentData.data),
            this.currentData.yData,
            this.currentData.xData,
            this.currentData.missingData,
            0);
    }

    changeBuckets(bucketCount: number): void {
        /* TODO: use sampling here too
        let xSamplingRate = HistogramViewBase.samplingRate(bucketCount, this.currentData.visiblePoints, this.page);
        let ySamplingRate = HistogramViewBase.samplingRate(this.currentData.yPoints, this.currentData.visiblePoints, this.page);
        */
        let samplingRate = 1; // Math.max(xSamplingRate, ySamplingRate);

        let xBoundaries, yBoundaries;
        if (this.currentData.xData.distinctStrings == null)
            xBoundaries = null;
        else
            xBoundaries = this.currentData.xData.distinctStrings.uniqueStrings;
        if (this.currentData.yData.distinctStrings == null)
            yBoundaries = null;
        else
            yBoundaries = this.currentData.yData.distinctStrings.uniqueStrings;

        let arg0: ColumnAndRange = {
            columnName: this.currentData.xData.description.name,
            min: this.currentData.xData.stats.min,
            max: this.currentData.xData.stats.max,
            bucketCount: bucketCount,
            samplingRate: samplingRate,
            cdfBucketCount: 0,
            bucketBoundaries: xBoundaries
        };
        let arg1: ColumnAndRange = {
            columnName: this.currentData.yData.description.name,
            min: this.currentData.yData.stats.min,
            max: this.currentData.yData.stats.max,
            samplingRate: samplingRate,
            bucketCount: this.currentData.yPoints,
            cdfBucketCount: 0,
            bucketBoundaries: yBoundaries
        };
        let rr = this.createHeatMapRequest(arg0, arg1);
        let renderer = new Histogram2DRenderer(this.page,
            this.remoteObjectId, this.tableSchema,
            [this.currentData.xData.description, this.currentData.yData.description],
            [this.currentData.xData.stats, this.currentData.yData.stats],
            [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
            rr);
        rr.invoke(renderer);
    }

    chooseBuckets(): void {
        if (this.currentData == null)
            return;

        let bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
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
        this.page.reportError("Operation took " + significantDigits(elapsedMs / 1000) + " seconds");
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
            visiblePoints: 0,
            xPoints: xPoints,
            yPoints: yRectangles
        };

        let canvasSize = Resolution.getCanvasSize(this.page);
        this.chartSize = Resolution.getChartSize(this.page);

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
        let rects: Rect[] = [];
        for (let x = 0; x < data.length; x++) {
            let yTotal = 0;
            for (let y = 0; y < data[x].length; y++) {
                let v = data[x][y];
                this.currentData.visiblePoints += v;
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
            .attr("width", canvasSize.width)
            .attr("border", 1)
            .attr("height", canvasSize.height)
            .attr("cursor", "crosshair");

        this.canvas.on("mousemove", () => this.onMouseMove());

        // The chart uses a fragment of the canvas offset by the margins
        this.chart = this.canvas
            .append("g")
            .attr("transform", translateString(Resolution.leftMargin, Resolution.topMargin));

        this.yScale = d3.scaleLinear()
            .range([this.chartSize.height, 0]);
        if (this.normalized)
            this.yScale.domain([0, 100]);
        else
            this.yScale.domain([0, max]);
        let yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let cd = xData.description;
        let bucketCount = xPoints;
        let scAxis = HistogramViewBase.createScaleAndAxis(cd.kind, bucketCount, this.chartSize.width,
            xData.stats.min, xData.stats.max, xData.distinctStrings, true);
        this.xScale = scAxis.scale;
        let xAxis = scAxis.axis;

        this.canvas.append("text")
            .text(xData.description.name)
            .attr("transform", translateString(this.chartSize.width / 2,
                this.chartSize.height + Resolution.topMargin + Resolution.bottomMargin))
            .attr("text-anchor", "middle");
        this.canvas.append("text")
            .text(yData.description.name)
            .attr("transform", translateString(this.chartSize.width / 2, 0))
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");

        let barWidth = this.chartSize.width / bucketCount;
        let scale = this.chartSize.height / max;
        let pixelHeight = max / this.chartSize.height;

        this.chart.selectAll("g")
        // bars
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", d => d.x * barWidth)
            .attr("y", d => this.rectPosition(d, counts, scale, this.chartSize.height))
            .attr("height", d => this.rectHeight(d, counts, scale, this.chartSize.height))
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
            .attr("y", d => this.chartSize.height - (d * scale))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text(d => HistogramViewBase.boxHeight(d, true, pixelHeight))
            .exit();

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        if (xAxis != null) {
            this.chart.append("g")
                .attr("class", "x-axis")
                .attr("transform", translateString(0, this.chartSize.height))
                .call(xAxis);
        }

        let dotRadius = 3;
        this.xDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cy", this.chartSize.height + Resolution.topMargin)
            .attr("cx", 0)
            .attr("fill", "blue");
        this.yDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("cx", Resolution.leftMargin)
            .attr("cy", 0)
            .attr("fill", "blue");
        this.cdfDot = this.canvas
            .append("circle")
            .attr("r", dotRadius)
            .attr("fill", "blue");

        this.legendRect = this.legendRectangle();
        let legendSvg = this.canvas
            .append("svg");

        // apparently SVG defs are global, even if they are in
        // different SVG elements.  So we have to assign unique names.
        let gradientId = 'gradient' + this.getPage().pageId;
        let gradient = legendSvg.append('defs')
            .append('linearGradient')
            .attr('id', gradientId)
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

        this.legend = legendSvg.append("rect")
            .attr("width", this.legendRect.width())
            .attr("height", this.legendRect.height())
            .style("fill", "url(#" + gradientId + ")")
            .attr("x", this.legendRect.upperLeft().x)
            .attr("y", this.legendRect.upperLeft().y);

        let legendBuckets = this.currentData.data[0].length;
        let scaleAxis = HistogramViewBase.createScaleAndAxis(
            this.currentData.yData.description.kind, legendBuckets, this.legendRect.width(),
            this.currentData.yData.stats.min, this.currentData.yData.stats.max,
            this.currentData.yData.distinctStrings, true);

        // create a scale and axis for the legend
        this.legendScale = scaleAxis.scale;
        let legendAxis = scaleAxis.axis;
        legendSvg.append("g")
            .attr("transform", translateString(this.legendRect.lowerLeft().x, this.legendRect.lowerLeft().y))
            .call(legendAxis);

        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);

        let summary = formatNumber(this.currentData.visiblePoints) + " data points";
        if (missingData != 0)
            summary += ", " + formatNumber(missingData) + " missing";
        if (xData.missing.missingData != 0)
            summary += ", " + formatNumber(xData.missing.missingData) + " missing Y coordinate";
        if (yData.missing.missingData != 0)
            summary += ", " + formatNumber(yData.missing.missingData) + " missing X coordinate";
        summary += ", " + String(bucketCount) + " buckets";
        this.summary.textContent = summary;
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
        if (this.currentData.xData.description.kind == "Category")
            xs = this.currentData.xData.distinctStrings.get(<number>x);
        else if (this.currentData.xData.description.kind == "Integer" ||
            this.currentData.xData.description.kind == "Double")
            xs = significantDigits(<number>x);
        let y = Math.round(this.yScale.invert(position[1]));
        let ys = significantDigits(y);
        this.xLabel.textContent = "x=" + xs;
        this.yLabel.textContent = "y=" + ys;

        this.xDot.attr("cx", mouseX + Resolution.leftMargin);
        this.yDot.attr("cy", mouseY + Resolution.topMargin);
    }

    protected legendRectangle(): Rectangle {
        let width = Resolution.legendSize.width;
        if (width > this.chartSize.width)
            width = this.chartSize.width;
        let height = 15;

        let x = (this.chartSize.width - width) / 2;
        let y = Resolution.topMargin / 3;
        return new Rectangle({ x: x, y: y }, { width: width, height: height });
    }

    // We support two kinds of selection:
    // - selection of bars in the histogram area
    // - selection in the legend area
    // We distinguish the two by the original mouse position: if the mouse
    // is above the chart, we are selecting in the legend.
    protected dragStart() {
        super.dragStart();
        this.selectingLegend = this.legendRect != null && this.selectionOrigin.y < 0;
    }

    protected dragMove(): void {
        super.dragMove();
        if (!this.dragging)
            return;

        if (this.selectingLegend) {
            this.selectionRectangle
                .attr("y", this.legendRect.upperLeft().y)
                .attr("height", this.legendRect.height());
        }
    }

    // xl and xr are coordinates of the mouse position within the chart
    protected selectionCompleted(xl: number, xr: number): void {
        if (this.xScale == null)
            return;

        let min: number;
        let max: number;
        let boundaries: string[] = null;
        let selectedAxis: AxisData = null;
        let scale: AnyScale = null;

        if (this.selectingLegend) {
            // Selecting in legend.  We have to adjust xl and xr, they are relative to the chart.
            // The legend rectangle coordinates are relative to the canvas.
            let legendX = this.legendRect.lowerLeft().x;
            xl -= legendX - Resolution.leftMargin;
            xr -= legendX - Resolution.leftMargin;
            selectedAxis = this.currentData.yData;
            scale = this.legendScale;
        } else {
            selectedAxis = this.currentData.xData;
            scale = this.xScale;
        }

        let kind = selectedAxis.description.kind;
        let x0 = HistogramViewBase.invertToNumber(xl, scale, kind);
        let x1 = HistogramViewBase.invertToNumber(xr, scale, kind);

        // selection could be done in reverse
        [min, max] = reorder(x0, x1);
        if (min > max) {
            this.page.reportError("No data selected");
            return;
        }

        if (selectedAxis.distinctStrings != null)
            boundaries = selectedAxis.distinctStrings.categoriesInRange(min, max, max - min);
        let filter: FilterDescription = {
            min: min,
            max: max,
            columnName: selectedAxis.description.name,
            bucketBoundaries: boundaries,
            complement: d3.event.sourceEvent.ctrlKey
        };

        let rr = this.createFilterRequest(filter);
        let renderer = new Filter2DReceiver(
            this.currentData.xData.description,
            this.currentData.yData.description,
            this.currentData.xData.distinctStrings,
            this.currentData.yData.distinctStrings,
            this.tableSchema,
            this.page, rr, false);
        rr.invoke(renderer);
    }

    protected rectHeight(d: Rect, counts: number[], scale: number, chartHeight: number): number {
        if (this.normalized) {
            let c = counts[d.x];
            if (c <= 0)
                return 0;
            return chartHeight * d.height / c;
        }
        return d.height * scale;
    }

    protected rectPosition(d: Rect, counts: number[], scale: number, chartHeight: number): number {
        let y = d.y + d.height;
        if (this.normalized) {
            let c = counts[d.x];
            if (c <= 0)
                return 0;
            return chartHeight * (1 - y / c);
        }
        return chartHeight - y * scale;
    }

    static colorMap(d: number): string {
        // The rainbow color map starts and ends with a similar hue
        // so we skip the first 20% of it.
        return d3.interpolateRainbow(d * .8 + .2);
    }

    color(d: number, max: number): string {
        if (max == 0)
            return Histogram2DView.colorMap(0);
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
        page.setDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new TableRenderer(page, table, rr, false, order));
    }
}

export class Filter2DReceiver extends RemoteTableRenderer {
    constructor(protected xColumn: ColumnDescription,
                protected yColumn: ColumnDescription,
                protected xDs: DistinctStrings,
                protected yDs: DistinctStrings,
                protected tableSchema: Schema,
                page: FullPage,
                operation: ICancellable,
                protected heatMap: boolean) {
        super(page, operation, "Filter");
    }

    public onCompleted(): void {
        this.finished();
        if (this.remoteObject == null)
            return;

        let cds: ColumnDescription[] = [this.xColumn, this.yColumn];
        let ds: DistinctStrings[] = [this.xDs, this.yDs];
        let rx = new RangeInfo(this.xColumn.name, this.xDs != null ? this.xDs.uniqueStrings : null);
        let ry = new RangeInfo(this.yColumn.name, this.yDs != null ? this.yDs.uniqueStrings : null);
        let rr = this.remoteObject.createRange2DRequest(rx, ry);
        rr.invoke(new Range2DCollector(cds, this.tableSchema, ds, this.page, this.remoteObject, rr, this.heatMap));
    }
}

// This class is invoked by the ZipReceiver after a set operation to create a new histogram
export class Make2DHistogram extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable,
                       private colDesc: ColumnDescription[],
                       protected ds: DistinctStrings[],
                       private schema: Schema,
                       private heatMap: boolean) {
        super(page, operation, "Reload");
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteObject == null)
            return;

        let rx = new RangeInfo(this.colDesc[0].name, this.ds[0] != null ? this.ds[0].uniqueStrings : null);
        let ry = new RangeInfo(this.colDesc[1].name, this.ds[1] != null ? this.ds[1].uniqueStrings : null);
        let rr = this.remoteObject.createRange2DRequest(rx, ry);
        rr.chain(this.operation);
        rr.invoke(new Range2DCollector(
            this.colDesc, this.schema, this.ds, this.page, this.remoteObject, rr, this.heatMap));
    }
}

export class Histogram2DRenderer extends Renderer<HeatMapData> {
    protected histogram: Histogram2DView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                protected uniqueStrings: DistinctStrings[],
                operation: ICancellable) {
        super(new FullPage(), operation, "histogram");
        page.insertAfterMe(this.page);
        this.histogram = new Histogram2DView(remoteTableId, schema, this.page);
        this.page.setDataView(this.histogram);
        if (cds.length != 2 || stats.length != 2 || uniqueStrings.length != 2)
            throw "Expected 2 columns";
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

        let xAxisData = new AxisData(value.data.histogramMissingD1, this.cds[0], this.stats[0], this.uniqueStrings[0], xPoints);
        let yAxisData = new AxisData(value.data.histogramMissingD2, this.cds[1], this.stats[1], this.uniqueStrings[1], yPoints);
        this.histogram.updateView(value.data.buckets, xAxisData, yAxisData,
            value.data.missingData, this.elapsedMilliseconds());
        this.histogram.scrollIntoView();
    }
}

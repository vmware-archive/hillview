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
import {
    ColumnDescription, Schema, RecordOrder, ColumnAndRange, FilterDescription,
    BasicColStats, RangeInfo, Histogram2DArgs, CombineOperators, RemoteObjectId
} from "../javaBridge";
import {TopMenu, SubMenu} from "../ui/menu";
import {
    reorder, transpose, significantDigits, formatNumber, ICancellable, PartialResult, Seed
} from "../util";
import {Renderer} from "../rpc";
import {Rectangle, Resolution} from "../ui/ui";
import {FullPage} from "../ui/fullPage";
import {TextOverlay} from "../ui/textOverlay";
import {AnyScale, AxisData} from "./axisData";
import { HistogramViewBase, BucketDialog } from "./histogramViewBase";
import {TableView, NextKReceiver} from "./tableView";
import {HeatMapData, Range2DCollector} from "./heatMapView";
import {RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";

/**
 * Represents an SVG rectangle drawn on the screen.
 */
interface Rect {
    /**
     * Bucket index on the X axis.
     */
    xIndex: number;
    /**
     * Bucket index on the Y axis.
     */
    yIndex: number;
    /**
     * Count of items represented by the rectangle.
     */
    count: number;
    /**
     * Count of items below this rectangle.
     */
    countBelow: number;
}

/**
 * This class is responsible for rendering a 2D histogram.
 * This is a histogram where each bar is divided further into sub-bars.
 */
export class Histogram2DView extends HistogramViewBase {
    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        missingData: number;
        data: number[][];
        xPoints: number;
        yPoints: number;
        visiblePoints: number;
        samplingRate: number;
    };
    protected normalized: boolean;  // true when bars are normalized to 100%
    protected selectingLegend: boolean;  // true when selection is being done in legend
    protected legendRect: Rectangle;  // legend position on the screen; relative to canvas
    protected legend: any;  // a d3 object
    protected legendScale: AnyScale;
    protected menu: TopMenu;
    protected barWidth: number;  // in pixels
    protected max: number;  // maximum count displayed (total size of stacked bars)

    constructor(remoteObjectId: RemoteObjectId, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, tableSchema, page);
        this.menu = new TopMenu( [{
            text: "View",
            help: "Change the way the data is displayed.",
            subMenu: new SubMenu([{
                text: "refresh",
                action: () => { this.refresh(); },
                help: "Redraw this view"
            }, {
                text: "table",
                action: () => this.showTable(),
                help: "Show the data underlying this plot in a tabular view. "
            },{
                text: "exact",
                action: () => { this.exactHistogram(); },
                help: "Draw this histogram without approximations."
            },{
                text: "#buckets",
                action: () => this.chooseBuckets(),
                help: "Change the number of buckets used for drawing the histogram." +
                    "The number must be between 1 and " + Resolution.maxBucketCount
            }, {
                text: "swap axes",
                action: () => { this.swapAxes(); },
                help: "Redraw this histogram by swapping the X and Y axes."
            }, {
                text: "heatmap",
                action: () => { this.heatmap(); },
                help: "Plot this data as a heatmap view."
            }, {
                text: "relative/absolute",
                action: () => { this.normalized = !this.normalized; this.refresh(); },
                help: "In an absolute plot the Y axis represents the size for a bucket. " +
                "In a relative plot all bars are normalized to 100% on the Y axis."
            }]) }, {
                text: "Combine",
                help: "Combine data in two separate views.",
                subMenu: combineMenu(this, page.pageId)
            }
        ]);

        this.normalized = false;
        this.selectingLegend = true;
        this.page.setMenu(this.menu);
    }

    heatmap(): void {
        let rcol = new Range2DCollector([this.currentData.xData.description, this.currentData.yData.description],
            this.tableSchema, [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
            this.page, this, this.currentData.samplingRate >= 1, null, true);
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
                this.tableSchema, this.currentData.samplingRate >= 1, false);
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
            this.currentData.samplingRate,
            0);
    }

    exactHistogram(): void {
        if (this.currentData == null)
            return;
        let rc = new Range2DCollector(
            [this.currentData.xData.description, this.currentData.yData.description],
            this.tableSchema,
            [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
            this.page, this, true, null, false);
        rc.setValue({ first: this.currentData.xData.stats,
            second: this.currentData.yData.stats });
        rc.onCompleted();
    }

    changeBuckets(bucketCount: number): void {
        let samplingRate = HistogramViewBase.samplingRate(bucketCount,
            this.currentData.xData.stats.presentCount, this.page);

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
            bucketBoundaries: xBoundaries
        };
        let arg1: ColumnAndRange = {
            columnName: this.currentData.yData.description.name,
            min: this.currentData.yData.stats.min,
            max: this.currentData.yData.stats.max,
            bucketBoundaries: yBoundaries
        };
        let args: Histogram2DArgs = {
            first: arg0,
            second: arg1,
            xBucketCount: bucketCount,
            yBucketCount: this.currentData.yPoints,
            samplingRate: samplingRate,
            seed: Seed.instance.get()
        };
        let rr = this.createHeatMapRequest(args);
        let renderer = new Histogram2DRenderer(this.page,
            this.remoteObjectId, this.tableSchema,
            [this.currentData.xData.description, this.currentData.yData.description],
            [this.currentData.xData.stats, this.currentData.yData.stats],
            samplingRate,
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
            samplingRate: samplingRate,
            xPoints: xPoints,
            yPoints: yRectangles
        };

        let canvasSize = Resolution.getCanvasSize(this.page);
        this.chartSize = Resolution.getChartSize(this.page);

        // TODO: compute and draw CDF

        if (this.canvas != null)
            this.canvas.remove();

        let counts: number[] = [];
        let missingDisplayed: number = 0;

        this.max = 0;
        let rects: Rect[] = [];
        for (let x = 0; x < data.length; x++) {
            let yTotal = 0;
            for (let y = 0; y < data[x].length; y++) {
                let v = data[x][y];
                this.currentData.visiblePoints += v;
                if (v != 0) {
                    let rec: Rect = {
                        xIndex: x,
                        countBelow: yTotal,
                        yIndex: y,
                        count: v
                    };
                    rects.push(rec);
                }
                yTotal += v;
            }
            let v = yData.missing.buckets[x];
            let rec: Rect = {
                xIndex: x,
                countBelow: yTotal,
                yIndex: data[x].length,
                count: v
            };
            rects.push(rec);
            yTotal += v;
            missingDisplayed += v;
            if (yTotal > this.max)
                this.max = yTotal;
            counts.push(yTotal);
        }

        let noX = 0;
        for (let y = 0; y < xData.missing.buckets.length; y++)
            noX += xData.missing.buckets[y];

        if (this.max <= 0) {
            this.page.reportError("All values are missing: " + noX + " have no X value, "
                + missingData + " have no X or Y value");
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
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`);

        this.yScale = d3.scaleLinear()
            .range([this.chartSize.height, 0]);
        if (this.normalized)
            this.yScale.domain([0, 100]);
        else
            this.yScale.domain([0, this.max]);
        let yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let bucketCount = xPoints;
        let scAxis = xData.scaleAndAxis(this.chartSize.width, true, false);
        this.xScale = scAxis.scale;
        let xAxis = scAxis.axis;

        this.canvas.append("text")
            .text(xData.description.name)
            .attr("transform", `translate(${this.chartSize.width / 2},
                ${this.chartSize.height + Resolution.topMargin + Resolution.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");

        this.barWidth = this.chartSize.width / bucketCount;
        let scale = this.chartSize.height / this.max;

        this.chart.selectAll("g")
             // bars
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", (d: Rect) => d.xIndex * this.barWidth)
            .attr("y", (d: Rect) => this.rectPosition(d, counts, scale, this.chartSize.height))
            .attr("height", (d: Rect) => this.rectHeight(d, counts, scale, this.chartSize.height))
            .attr("width", this.barWidth - 1)
            .attr("fill", (d: Rect) => this.color(d.yIndex, yRectangles - 1))
            .attr("stroke", "black")
            .attr("stroke-width", (d: Rect) => d.yIndex > yRectangles - 1 ? 1 : 0)
            .exit()
            // label bars
            .data(counts)
            .enter()
            .append("g")
            .append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", (c, i: number) => (i + .5) * this.barWidth)
            .attr("y", (d: number) => this.normalized ? 0 : this.chartSize.height - (d * scale))
            .attr("text-anchor", "middle")
            .attr("dy", (d: number) => this.normalized ? 0 : d <= (9 * this.max / 10) ? "-.25em" : ".75em")
            .text((d: number) => HistogramViewBase.boxHeight(d, this.currentData.samplingRate,
                this.currentData.visiblePoints))
            .exit();

        this.chart.append("g")
            .attr("class", "y-axis")
            .call(yAxis);
        if (xAxis != null) {
            this.chart.append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(0, ${this.chartSize.height})`)
                .call(xAxis);
        }

        this.cdfDot = this.canvas
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");

        if (missingDisplayed > 0 || this.currentData.yData.stats.max > this.currentData.yData.stats.min)
            this.canvas.append("text")
                .text(yData.description.name)
                .attr("transform", `translate(${this.chartSize.width / 2}, 0)`)
                .attr("text-anchor", "middle")
                .attr("dominant-baseline", "text-before-edge");

        if (this.currentData.yData.stats.max > this.currentData.yData.stats.min) {
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

            let scaleAxis = this.currentData.yData.scaleAndAxis(this.legendRect.width(), true, true);

            // create a scale and axis for the legend
            this.legendScale = scaleAxis.scale;
            let legendAxis = scaleAxis.axis;
            legendSvg.append("g")
                .attr("transform", `translate(${this.legendRect.lowerLeft().x},
                                              ${this.legendRect.lowerLeft().y})`)
                .call(legendAxis);
        }

        if (missingDisplayed > 0) {
            let missingGap = 20;
            let missingWidth = 20;
            let missingHeight = 15;
            let missingX = 0;
            let missingY = 0;
            if (this.legendRect != null) {
                missingX = this.legendRect.upperRight().x + missingGap;
                missingY = this.legendRect.upperRight().y;
            } else {
                missingX = this.chartSize.width / 2;
                missingY = Resolution.topMargin / 3;
            }
            let missingSvg = this.canvas
                .append("svg");

            missingSvg.append("rect")
                .attr("width", missingWidth)
                .attr("height", missingHeight)
                .attr("x", missingX)
                .attr("y", missingY)
                .attr("stroke", "black")
                .attr("fill", "none")
                .attr("stroke-width", 1);

            this.canvas.append("text")
                .text("missing")
                .attr("transform", `translate(${missingX + missingWidth / 2}, ${missingY + missingHeight + 7})`)
                .attr("text-anchor", "middle")
                .attr("font-size", 10)
                .attr("dominant-baseline", "text-before-edge");
        }

        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);

        this.pointDescription = new TextOverlay(this.chart,
            [this.currentData.xData.description.name, "y",
                this.currentData.yData.description.name, "count"], 40);
        let summary = formatNumber(this.currentData.visiblePoints) + " data points";
        if (missingData != 0)
            summary += ", " + formatNumber(missingData) + " missing";
        if (xData.missing.missingData != 0)
            summary += ", " + formatNumber(xData.missing.missingData) + " missing Y coordinate";
        if (yData.missing.missingData != 0)
            summary += ", " + formatNumber(yData.missing.missingData) + " missing X coordinate";
        summary += ", " + String(bucketCount) + " buckets";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
    }

    protected onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = HistogramViewBase.invert(position[0], this.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.distinctStrings);
        let y = Math.round(this.yScale.invert(mouseY));
        let ys = significantDigits(y);
        let scale = 1.0;
        if (this.normalized)
            ys += "%";

        // Find out the rectangle where the mouse is
        let value = "", size = "";
        let xIndex = Math.floor(mouseX / this.barWidth);
        if (xIndex >= 0 && xIndex < this.currentData.data.length &&
            y >= 0 && mouseY < this.chartSize.height) {
            let values: number[] = this.currentData.data[xIndex];

            let total = 0;
            for (let i = 0; i < values.length; i++)
                total += values[i];
            total += this.currentData.yData.missing.buckets[xIndex];
            if (total > 0) {
                // There could be no data for this specific x value
                if (this.normalized)
                    scale = 100 / total;

                let yTotal = 0;
                for (let i = 0; i < values.length; i++) {
                    yTotal += values[i] * scale;
                    if (yTotal >= y) {
                        size = significantDigits(values[i]);
                        value = this.currentData.yData.bucketDescription(i);
                        break;
                    }
                }
                let missing = this.currentData.yData.missing.buckets[xIndex] * scale;
                if (value == "" && yTotal + missing >= y) {
                    value = "missing";
                    size = significantDigits(missing);
                }
            }
            // else value is ""
        }

        this.pointDescription.update([xs, ys, value, size], mouseX, mouseY);
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
            // Prevent the selection from being larger than the legend.
            let minSel: number = +this.selectionRectangle.attr("x");
            let width: number = +this.selectionRectangle.attr("width");
            if (minSel < this.legendRect.origin.x) {
                let delta = this.legendRect.origin.x - minSel;
                this.selectionRectangle
                    .attr("x", this.legendRect.origin.x)
                    .attr("width", width - delta);
            } else if (minSel + width > this.legendRect.lowerRight().x) {
                let delta = minSel + width - this.legendRect.lowerRight().x;
                this.selectionRectangle
                    .attr("width", width - delta);
            }

            // Move the selection rectangle in the legend.
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
            kind: selectedAxis.description.kind,
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
            this.page, this.currentData.samplingRate >= 1.0, rr, false);
        rr.invoke(renderer);
    }

    protected rectHeight(d: Rect, counts: number[], scale: number, chartHeight: number): number {
        if (this.normalized) {
            let c = counts[d.xIndex];
            if (c <= 0)
                return 0;
            return chartHeight * d.count / c;
        }
        return d.count * scale;
    }

    protected rectPosition(d: Rect, counts: number[], scale: number, chartHeight: number): number {
        let y = d.countBelow + d.count;
        if (this.normalized) {
            let c = counts[d.xIndex];
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
        if (d > max)
            // This is for the "missing" data
            return "none";
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
        let page = new FullPage("Table", "Table", this.page);
        page.setDataView(table);
        this.page.insertAfterMe(page);
        rr.invoke(new NextKReceiver(page, table, rr, false, order));
    }
}

/**
 * Receives the result of a filtering operation on two axes and initiates
 * a new 2D range computation, which in turns initiates a new 2D histogram
 * rendering.
 */
export class Filter2DReceiver extends RemoteTableRenderer {
    constructor(protected xColumn: ColumnDescription,
                protected yColumn: ColumnDescription,
                protected xDs: DistinctStrings,
                protected yDs: DistinctStrings,
                protected tableSchema: Schema,
                page: FullPage,
                protected exact: boolean,
                operation: ICancellable,
                protected heatMap: boolean) {
        super(page, operation, "Filter");
    }

    public run(): void {
        super.run();
        let cds: ColumnDescription[] = [this.xColumn, this.yColumn];
        let ds: DistinctStrings[] = [this.xDs, this.yDs];
        let rx = new RangeInfo(this.xColumn.name, this.xDs != null ? this.xDs.uniqueStrings : null);
        let ry = new RangeInfo(this.yColumn.name, this.yDs != null ? this.yDs.uniqueStrings : null);
        let rr = this.remoteObject.createRange2DRequest(rx, ry);
        rr.invoke(new Range2DCollector(cds, this.tableSchema, ds, this.page, this.remoteObject, this.exact, rr, this.heatMap));
    }
}

/**
 * This class is invoked by the ZipReceiver after a set operation
 * to create a new 2D histogram.
  */
export class Make2DHistogram extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable,
                       private colDesc: ColumnDescription[],
                       protected ds: DistinctStrings[],
                       private schema: Schema,
                       private exact: boolean,
                       private heatMap: boolean) {
        super(page, operation, "Reload");
    }

    run(): void {
        super.run();
        let rx = new RangeInfo(this.colDesc[0].name, this.ds[0] != null ? this.ds[0].uniqueStrings : null);
        let ry = new RangeInfo(this.colDesc[1].name, this.ds[1] != null ? this.ds[1].uniqueStrings : null);
        let rr = this.remoteObject.createRange2DRequest(rx, ry);
        rr.chain(this.operation);
        rr.invoke(new Range2DCollector(
            this.colDesc, this.schema, this.ds, this.page, this.remoteObject, this.exact, rr, this.heatMap));
    }
}

/**
 * Receives partial results and renders a 2D histogram.
 * The 2D histogram data and the HeatMap data use the same data structure.
 */
export class Histogram2DRenderer extends Renderer<HeatMapData> {
    protected histogram: Histogram2DView;

    constructor(page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                protected samplingRate: number,
                protected uniqueStrings: DistinctStrings[],
                operation: ICancellable) {
        super(new FullPage("2D Histogram " + cds[0].name + ", " + cds[1].name, "2DHistogram", page),
            operation, "histogram");
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
            value.data.missingData, this.samplingRate, this.elapsedMilliseconds());
    }
}

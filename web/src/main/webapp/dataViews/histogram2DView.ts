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
    BasicColStats, RangeInfo, Histogram2DArgs, CombineOperators, RemoteObjectId, HeatMap, Histogram
} from "../javaBridge";
import {TopMenu, SubMenu} from "../ui/menu";
import {
    reorder, transpose, significantDigits, formatNumber, ICancellable, PartialResult, Seed, Pair, percent
} from "../util";
import {Renderer} from "../rpc";
import {Rectangle, Resolution} from "../ui/ui";
import {FullPage} from "../ui/fullPage";
import {TextOverlay} from "../ui/textOverlay";
import {AnyScale, AxisData} from "./axisData";
import { HistogramViewBase, BucketDialog } from "./histogramViewBase";
import {TableView, NextKReceiver} from "./tableView";
import {Range2DCollector} from "./heatMapView";
import {RemoteTableObject, RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";
import {CDFPlot} from "../ui/CDFPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {Histogram2DPlot} from "../ui/Histogram2DPlot";
import {HistogramLegendPlot} from "../ui/legendPlot";

/**
 * This class is responsible for rendering a 2D histogram.
 * This is a histogram where each bar is divided further into sub-bars.
 */
export class Histogram2DView extends HistogramViewBase {
    protected currentData: {
        xData: AxisData;
        yData: AxisData;
        cdf: Histogram;
        heatMap: HeatMap;
        xPoints: number;
        yPoints: number;
        samplingRate: number;
    };
    protected normalized: boolean;  // true when bars are normalized to 100%
    protected legendRect: Rectangle;  // legend position on the screen; relative to canvas
    protected menu: TopMenu;
    protected legendSelectionRectangle: any;
    protected plot: Histogram2DPlot;
    protected legendPlot: HistogramLegendPlot;
    protected legendSurface: PlottingSurface;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId,
                protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, originalTableId, tableSchema, page);

        this.legendSurface = new PlottingSurface(this.chartDiv, page);
        //this.legendSurface.setMargins(0, 0, 0, 0);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.legendPlot = new HistogramLegendPlot(this.legendSurface);
        this.surface = new PlottingSurface(this.chartDiv, page);
        this.plot = new Histogram2DPlot(this.surface);
        this.cdfPlot = new CDFPlot(this.surface);

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
        this.page.setMenu(this.menu);
    }

    public updateView(heatmap: HeatMap, xData: AxisData, yData: AxisData, cdf: Histogram,
                      samplingRate: number, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        this.legendPlot.clear();
        if (heatmap == null || heatmap.buckets.length == 0) {
            this.page.reportError("No data to display");
            return;
        }
        if (samplingRate >= 1) {
            let submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
        let xPoints = heatmap.buckets.length;
        let yPoints = heatmap.buckets[0].length;
        if (yPoints == 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.currentData = {
            heatMap: heatmap,
            xData: xData,
            yData: yData,
            cdf: cdf,
            samplingRate: samplingRate,
            xPoints: xPoints,
            yPoints: yPoints
        };

        let bucketCount = xPoints;
        let canvas = this.surface.getCanvas();

        let legendDrag = d3.drag()
            .on("start", () => this.dragLegendStart())
            .on("drag", () => this.dragLegendMove())
            .on("end", () => this.dragLegendEnd());
        this.legendSurface.getCanvas()
            .call(legendDrag);

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragCanvasEnd());

        this.plot.setData(heatmap, cdf, xData, yData, samplingRate, this.normalized);
        this.plot.draw();
        this.cdfPlot.setData(cdf);
        this.cdfPlot.draw();
        this.legendPlot.setData(yData, this.plot.getMissingDisplayed() > 0);
        this.legendPlot.draw();

        canvas.call(drag)
            .on("mousemove", () => this.mouseMove())
            .on("mouseleave", () => this.mouseLeave())
            .on("mouseenter", () => this.mouseEnter());

        this.cdfDot = canvas
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");

        this.legendRect = this.legendPlot.legendRectangle();

        this.selectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);
        this.legendSelectionRectangle = this.legendSurface.getCanvas()
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);

        this.pointDescription = new TextOverlay(this.surface.getChart(),
            [this.currentData.xData.description.name, "y",
                this.currentData.yData.description.name, "count", "cdf"], 40);
        this.pointDescription.show(false);
        let summary = formatNumber(this.plot.getVisiblePoints()) + " data points";
        if (heatmap.missingData != 0)
            summary += ", " + formatNumber(heatmap.missingData) + " missing both coordinates";
        if (heatmap.histogramMissingX.missingData != 0)
            summary += ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate";
        if (heatmap.histogramMissingY.missingData != 0)
            summary += ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate";
        summary += ", " + String(bucketCount) + " buckets";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
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
        let r = SelectedObject.instance.getSelected(this, this.page.getErrorReporter());
        if (r == null)
            return;

        let rr = this.createZipRequest(r);
        let renderer = (page: FullPage, operation: ICancellable) => {
            return new Make2DHistogram(
                page, operation,
                [this.currentData.xData.description, this.currentData.yData.description],
                [this.currentData.xData.distinctStrings, this.currentData.yData.distinctStrings],
                this.tableSchema, this.currentData.samplingRate >= 1, false, this.originalTableId);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.originalTableId, renderer));
    }

    public swapAxes(): void {
        if (this.currentData == null)
            return;
        let rc = new Range2DCollector(
            [this.currentData.yData.description, this.currentData.xData.description],
            this.tableSchema,
            [this.currentData.yData.distinctStrings, this.currentData.xData.distinctStrings],
            this.page, this, true, null, false);
        rc.setValue({ first: this.currentData.yData.stats, second: this.currentData.xData.stats });
        rc.onCompleted();
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
        let size = PlottingSurface.getDefaultChartSize(this.page);
        let cdfCount = Math.floor(size.width);

        let args: Histogram2DArgs = {
            first: arg0,
            second: arg1,
            xBucketCount: bucketCount,
            yBucketCount: this.currentData.yPoints,
            samplingRate: samplingRate,
            seed: Seed.instance.get(),
            cdfBucketCount: cdfCount,
            cdfSamplingRate: HistogramViewBase.samplingRate(bucketCount,
                this.currentData.xData.stats.presentCount, this.page)
        };
        let rr = this.createHeatMapRequest(args);
        let renderer = new Histogram2DRenderer(this.page,
            this, this.tableSchema,
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
            this.currentData.heatMap,
            this.currentData.xData,
            this.currentData.yData,
            this.currentData.cdf,
            this.currentData.samplingRate,
            0);
    }

    public mouseEnter(): void {
        super.mouseEnter();
        this.cdfDot.attr("visibility", true);
    }

    public mouseLeave(): void {
        this.cdfDot.attr("visibility", false);
        super.mouseLeave();
    }

    /**
     * Handles mouse movements in the canvas area only.
     */
    public mouseMove(): void {
        let position = d3.mouse(this.surface.getChart().node());
        // note: this position is within the chart
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = HistogramViewBase.invert(position[0], this.plot.xScale,
            this.currentData.xData.description.kind, this.currentData.xData.distinctStrings);
        let y = Math.round(this.plot.yScale.invert(mouseY));
        let ys = significantDigits(y);
        let scale = 1.0;
        if (this.normalized)
            ys += "%";

        // Find out the rectangle where the mouse is
        let value = "", size = "";
        let xIndex = Math.floor(mouseX / this.plot.getBarWidth());
        if (xIndex >= 0 && xIndex < this.currentData.heatMap.buckets.length &&
            y >= 0 && mouseY < this.surface.getActualChartHeight()) {
            let values: number[] = this.currentData.heatMap.buckets[xIndex];

            let total = 0;
            for (let i = 0; i < values.length; i++)
                total += values[i];
            total += this.currentData.heatMap.histogramMissingY.buckets[xIndex];
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
                let missing = this.currentData.heatMap.histogramMissingY.buckets[xIndex] * scale;
                if (value == "" && yTotal + missing >= y) {
                    value = "missing";
                    size = significantDigits(missing);
                }
            }
            // else value is ""
        }

        let pos = this.cdfPlot.getY(mouseX);
        this.cdfDot.attr("cx", mouseX + this.surface.leftMargin);
        this.cdfDot.attr("cy", (1 - pos) * this.surface.getActualChartHeight() + this.surface.topMargin);
        let perc = percent(pos);
        this.pointDescription.update([xs, ys, value, size, perc], mouseX, mouseY);
    }

    protected dragCanvasEnd() {
        let dragging = this.dragging && this.moved;
        super.dragEnd();
        if (!dragging)
            return;
        let position = d3.mouse(this.surface.getCanvas().node());
        let x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x, false);
    }

    // dragging in the legend
   protected dragLegendStart() {
       this.dragging = true;
       this.moved = false;
       let position = d3.mouse(this.legendSurface.getCanvas().node());
       this.selectionOrigin = {
           x: position[0],
           y: position[1] };
    }

    protected dragLegendMove(): void {
        if (!this.dragging)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let position = d3.mouse(this.legendSurface.getCanvas().node());
        let x = position[0];
        let width = x - ox;
        let height = this.legendRect.height();

        if (width < 0) {
            ox = x;
            width = -width;
        }
        this.legendSelectionRectangle
            .attr("x", ox)
            .attr("width", width)
            .attr("y", this.legendRect.upperLeft().y)
            .attr("height", height);

        // Prevent the selection from spilling out of the legend itself
        if (ox < this.legendRect.origin.x) {
            let delta = this.legendRect.origin.x - ox;
            this.legendSelectionRectangle
                .attr("x", this.legendRect.origin.x)
                .attr("width", width - delta);
        } else if (ox + width > this.legendRect.lowerRight().x) {
            let delta = ox + width - this.legendRect.lowerRight().x;
            this.legendSelectionRectangle
                .attr("width", width - delta);
        }
    }

    protected dragLegendEnd(): void {
        if (!this.dragging || !this.moved)
            return;
        this.dragging = false;
        this.moved = false;
        this.legendSelectionRectangle
            .attr("width", 0)
            .attr("height", 0);

        let position = d3.mouse(this.legendSurface.getCanvas().node());
        let x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x, true);
    }

    protected cancelDrag() {
        super.cancelDrag();
        this.legendSelectionRectangle
            .attr("width", 0)
            .attr("heigh", 0);
    }

    /**
     * * xl and xr are coordinates of the mouse position within the canvas or legendSvg respectively.
     */
    protected selectionCompleted(xl: number, xr: number, inLegend: boolean): void {
        let min: number;
        let max: number;
        let boundaries: string[] = null;
        let selectedAxis: AxisData = null;
        let scale: AnyScale = null;

        if (inLegend) {
            let legendX = this.legendRect.lowerLeft().x;
            xl -= legendX;
            xr -= legendX;
            selectedAxis = this.currentData.yData;
            scale = this.legendPlot.xScale;
        } else {
            xl -= this.surface.leftMargin;
            xr -= this.surface.leftMargin;
            selectedAxis = this.currentData.xData;
            scale = this.plot.xScale;
        }

        if (scale == null)
            return;

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
            this.page, this.currentData.samplingRate >= 1.0, rr, false,
            this.originalTableId);
        rr.invoke(renderer);
    }

   static colorMap(d: number): string {
        // The rainbow color map starts and ends with a similar hue
        // so we skip the first 20% of it.
        return d3.interpolateRainbow(d * .8 + .2);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        let order =  new RecordOrder([ {
            columnDescription: this.currentData.xData.description,
            isAscending: true
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true
        } ]);

        let page = new FullPage("Table", "Table", this.page);
        let table = new TableView(this.remoteObjectId, this.originalTableId, page);
        table.setSchema(this.tableSchema);
        let rr = table.createNextKRequest(order, null);
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
                protected heatMap: boolean,
                originalTableId: RemoteObjectId) {
        super(page, operation, "Filter", originalTableId);
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
                       private heatMap: boolean,
                       originalTableId: RemoteObjectId) {
        super(page, operation, "Reload", originalTableId);
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
export class Histogram2DRenderer extends Renderer<Pair<HeatMap, Histogram>> {
    protected histogram: Histogram2DView;

    constructor(page: FullPage,
                protected remoteObject: RemoteTableObject,
                protected schema: Schema,
                protected cds: ColumnDescription[],
                protected stats: BasicColStats[],
                protected samplingRate: number,
                protected uniqueStrings: DistinctStrings[],
                operation: ICancellable) {
        super(new FullPage("2D Histogram " + cds[0].name + ", " + cds[1].name, "2DHistogram", page),
            operation, "histogram");
        page.insertAfterMe(this.page);
        this.histogram = new Histogram2DView(
            this.remoteObject.remoteObjectId, this.remoteObject.originalTableId, schema, this.page);
        this.page.setDataView(this.histogram);
        if (cds.length != 2 || stats.length != 2 || uniqueStrings.length != 2)
            throw "Expected 2 columns";
    }

    onNext(value: PartialResult<Pair<HeatMap, Histogram>>): void {
        super.onNext(value);
        if (value == null)
            return;
        let heatMap = value.data.first;
        let cdf = value.data.second;
        let points = heatMap.buckets;
        let xPoints = 1;
        let yPoints = 1;
        if (points != null) {
            xPoints = points.length;
            yPoints = points[0] != null ? points[0].length : 1;
        }

        let xAxisData = new AxisData(this.cds[0], this.stats[0], this.uniqueStrings[0], xPoints);
        let yAxisData = new AxisData(this.cds[1], this.stats[1], this.uniqueStrings[1], yPoints);
        this.histogram.updateView(heatMap, xAxisData, yAxisData, cdf,
            this.samplingRate, this.elapsedMilliseconds());
    }
}

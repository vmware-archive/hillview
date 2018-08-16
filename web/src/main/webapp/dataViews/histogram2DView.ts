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

import {drag as d3drag} from "d3-drag";
import {interpolateRainbow as d3interpolateRainbow} from "d3-scale-chromatic";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {DatasetView, Histogram2DSerialization, IViewSerialization} from "../datasetView";
import {
    CombineOperators,
    DataRange,
    FilterDescription,
    HeatMap,
    HistogramArgs,
    HistogramBase,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {OnCompleteReceiver, Receiver, RpcRequest} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, TableTargetAPI, ZipReceiver} from "../tableTarget";
import {CDFPlot} from "../ui/CDFPlot";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {Histogram2DPlot} from "../ui/Histogram2DPlot";
import {HistogramLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {D3SvgElement, Rectangle, Resolution} from "../ui/ui";
import {
    formatNumber,
    ICancellable,
    Pair,
    PartialResult,
    percent,
    reorder,
    saveAs,
    significantDigits,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {ChartOptions} from "./tsViewBase";
import {HeatMapRenderer} from "./heatmapView";

/**
 * This class is responsible for rendering a 2D histogram.
 * This is a histogram where each bar is divided further into sub-bars.
 */
export class Histogram2DView extends HistogramViewBase {
    protected currentData: {
        xAxisData: AxisData;
        yData: AxisData;
        cdf: HistogramBase;
        heatMap: HeatMap;
        xPoints: number;
        yPoints: number;
        samplingRate: number;
    };
    protected relative: boolean;  // true when bars are normalized to 100%
    protected legendRect: Rectangle;  // legend position on the screen; relative to canvas
    protected menu: TopMenu;
    protected legendSelectionRectangle: D3SvgElement;
    protected plot: Histogram2DPlot;
    protected legendPlot: HistogramLegendPlot;
    protected legendSurface: PlottingSurface;
    protected samplingRate: number;

    constructor(remoteObjectId: RemoteObjectId, rowCount: number,
                schema: SchemaClass, page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "2DHistogram");

        this.legendSurface = new PlottingSurface(this.chartDiv, page);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.legendPlot = new HistogramLegendPlot(this.legendSurface);
        this.surface = new PlottingSurface(this.chartDiv, page);
        this.plot = new Histogram2DPlot(this.surface);
        this.cdfPlot = new CDFPlot(this.surface);

        this.menu = new TopMenu( [{
           text: "Export",
           help: "Save the information in this view in a local file.",
           subMenu: new SubMenu([{
               text: "As CSV",
               help: "Saves the data in this view in a CSV file.",
               action: () => { this.export(); },
           }]),
        }, {
            text: "View",
            help: "Change the way the data is displayed.",
            subMenu: new SubMenu([{
                text: "refresh",
                action: () => { this.refresh(); },
                help: "Redraw this view",
            }, {
                text: "table",
                action: () => this.showTable(),
                help: "Show the data underlying this plot in a tabular view. ",
            }, {
                text: "exact",
                action: () => { this.exactHistogram(); },
                help: "Draw this histogram without approximations.",
            }, {
                text: "# buckets...",
                action: () => this.chooseBuckets(),
                help: "Change the number of buckets used for drawing the histogram." +
                    "The number must be between 1 and " + Resolution.maxBucketCount,
            }, {
                text: "swap axes",
                action: () => { this.swapAxes(); },
                help: "Redraw this histogram by swapping the X and Y axes.",
            }, {
                text: "heatmap",
                action: () => { this.heatmap(); },
                help: "Plot this data as a heatmap view.",
            }, {
                text: "relative/absolute",
                action: () => this.toggleNormalize(),
                help: "In an absolute plot the Y axis represents the size for a bucket. " +
                "In a relative plot all bars are normalized to 100% on the Y axis.",
            }]) },
            page.dataset.combineMenu(this, page.pageId),
        ]);

        this.relative = false;
        this.page.setMenu(this.menu);
    }

    public updateView(
        heatmap: HeatMap, xData: AxisData, yData: AxisData, cdf: HistogramBase,
        samplingRate: number, relative: boolean, elapsedMs: number): void {
        this.relative = relative;
        this.samplingRate = samplingRate;
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        this.legendPlot.clear();
        if (heatmap == null || heatmap.buckets.length === 0) {
            this.page.reportError("No data to display");
            return;
        }
        if (samplingRate >= 1) {
            const submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
        const xPoints = heatmap.buckets.length;
        const yPoints = heatmap.buckets[0].length;
        if (yPoints === 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.currentData = {
            heatMap: heatmap,
            xAxisData: xData,
            yData: yData,
            cdf: cdf,
            samplingRate: samplingRate,
            xPoints: xPoints,
            yPoints: yPoints
        };

        const bucketCount = xPoints;
        const canvas = this.surface.getCanvas();

        const legendDrag = d3drag()
            .on("start", () => this.dragLegendStart())
            .on("drag", () => this.dragLegendMove())
            .on("end", () => this.dragLegendEnd());
        this.legendSurface.getCanvas()
            .call(legendDrag);

        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragCanvasEnd());

        this.plot.setData(heatmap, cdf, xData, yData, samplingRate, this.relative);
        this.plot.draw();
        const discrete = kindIsString(this.currentData.xAxisData.description.kind) ||
            this.currentData.xAxisData.description.kind === "Integer";
        this.cdfPlot.setData(cdf, discrete);
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
            this.surface.getDefaultChartSize(),
            [   this.currentData.xAxisData.description.name,
                this.currentData.yData.description.name,
                "y", "count", "%", "cdf"], 40);
        this.pointDescription.show(false);
        let summary = formatNumber(this.plot.getDisplayedPoints()) + " data points";
        if (heatmap.missingData !== 0)
            summary += ", " + formatNumber(heatmap.missingData) + " missing both coordinates";
        if (heatmap.histogramMissingX.missingData !== 0)
            summary += ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate";
        if (heatmap.histogramMissingY.missingData !== 0)
            summary += ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate";
        summary += ", " + String(bucketCount) + " buckets";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
    }

    public serialize(): IViewSerialization {
        const result: Histogram2DSerialization = {
            ...super.serialize(),
            exact: this.currentData.samplingRate >= 1,
            relative: this.relative,
            columnDescription0: this.currentData.xAxisData.description,
            columnDescription1: this.currentData.yData.description,
            xBucketCount: this.currentData.xPoints,
        };
        return result;
    }

    public static reconstruct(ser: Histogram2DSerialization, page: FullPage): IDataView {
        const exact: boolean = ser.exact;
        const relative: boolean = ser.relative;
        const cd0: IColumnDescription = ser.columnDescription0;
        const cd1: IColumnDescription = ser.columnDescription1;
        const xPoints: number = ser.xBucketCount;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd0 === null || cd1 === null || exact === null || schema === null || xPoints === null)
            return null;
        const cds = [cd0, cd1];

        const hv = new Histogram2DView(ser.remoteObjectId, ser.rowCount, schema, page);
        const buckets = HistogramViewBase.histogram2DSize(page);
        const rr = hv.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(hv, hv.page, rr, hv.schema, xPoints, cds, null, {
            reusePage: true,
            relative: relative,
            heatmap: false,
            exact: exact
        }));
        return hv;
    }

    public toggleNormalize(): void {
        this.relative = !this.relative;
        if (this.relative && this.samplingRate < 1) {
            // We cannot use sampling when we display relative views.
            this.exactHistogram();
        } else {
            this.refresh();
        }
    }

    public heatmap(): void {
        const cds = [this.currentData.xAxisData.description, this.currentData.yData.description];
        const buckets = HistogramViewBase.heatmapSize(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: false,
            relative: false,
            heatmap: true,
            exact: true
        }));
    }

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "histogram2d.csv";
        saveAs(fileName, lines.join("\n"));
        this.page.reportError("Check the downloads folder for a file named '" + fileName + "'");
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [];
        let line = "";
        for (let y = 0; y < this.currentData.yData.cdfBucketCount; y++) {
            const by = this.currentData.yData.bucketDescription(y);
            line += "," + JSON.stringify(this.currentData.yData.description.name + " " + by);
        }
        line += ",missing";
        lines.push(line);
        for (let x = 0; x < this.currentData.xAxisData.cdfBucketCount; x++) {
            const data = this.currentData.heatMap.buckets[x];
            const bx = this.currentData.xAxisData.bucketDescription(x);
            let l = JSON.stringify(this.currentData.xAxisData.description.name + " " + bx);
            for (const y of data)
                l += "," + y;
            l += "," + this.currentData.heatMap.histogramMissingY.buckets[x];
            lines.push(l);
        }
        line = "mising";
        for (const y of this.currentData.heatMap.histogramMissingX.buckets)
            line += "," + y;
        lines.push(line);
        return lines;
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        const r = this.dataset.getSelected();
        if (r.first == null)
            return;

        const rr = this.createZipRequest(r.first);
        const renderer = (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new MakeHistogramOrHeatmap(
                page, operation,
                [this.currentData.xAxisData.description, this.currentData.yData.description],
                this.rowCount, this.schema,
                { exact: this.currentData.samplingRate >= 1, heatmap: false,
                    relative: this.relative, reusePage: false },
                this.dataset
                );
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.dataset, renderer));
    }

    public swapAxes(): void {
        if (this.currentData == null)
            return;
        const cds = [this.currentData.yData.description, this.currentData.xAxisData.description];
        const buckets = HistogramViewBase.histogram2DSize(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: true, relative: this.relative,
            heatmap: false, exact: this.samplingRate >= 1.0
        }));
    }

    public exactHistogram(): void {
        if (this.currentData == null)
            return;
        const cds = [this.currentData.yData.description, this.currentData.xAxisData.description];
        const buckets = HistogramViewBase.histogram2DSize(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            this.currentData.xPoints, cds, this.page.title, {
            reusePage: true,
            relative: this.relative,
            heatmap: false,
            exact: true
        }));
    }

    public changeBuckets(bucketCount: number): void {
        const cds = [this.currentData.yData.description, this.currentData.xAxisData.description];
        const buckets = HistogramViewBase.histogram2DSize(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            bucketCount, cds, null, {
            reusePage: true,
            relative: this.relative,
            heatmap: false,
            exact: true
        }));
    }

    public chooseBuckets(): void {
        if (this.currentData == null)
            return;

        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.heatMap,
            this.currentData.xAxisData,
            this.currentData.yData,
            this.currentData.cdf,
            this.currentData.samplingRate,
            this.relative,
            0);
        this.page.scrollIntoView();
    }

    public mouseEnter(): void {
        super.mouseEnter();
        this.cdfDot.attr("visibility", "visible");
    }

    public mouseLeave(): void {
        this.cdfDot.attr("visibility", "hidden");
        super.mouseLeave();
    }

    /**
     * Handles mouse movements in the canvas area only.
     */
    public mouseMove(): void {
        const position = d3mouse(this.surface.getChart().node());
        // note: this position is within the chart
        const mouseX = position[0];
        const mouseY = position[1];

        const xs = this.currentData.xAxisData.invert(position[0]);
        // Use the plot scale, not the yData to invert.  That's the
        // one which is used to draw the axis.
        const y = Math.round(this.plot.yScale.invert(mouseY));
        let ys = significantDigits(y);
        let scale = 1.0;
        if (this.relative)
            ys += "%";

        // Find out the rectangle where the mouse is
        let value = "";
        let size = "";
        const xIndex = Math.floor(mouseX / this.plot.getBarWidth());
        let perc: number = 0;
        let colorIndex: number = null;
        let found = false;
        if (xIndex >= 0 && xIndex < this.currentData.heatMap.buckets.length &&
            y >= 0 && mouseY < this.surface.getActualChartHeight()) {
            const values: number[] = this.currentData.heatMap.buckets[xIndex];

            let total = 0;
            for (const v of values)
                total += v;
            total += this.currentData.heatMap.histogramMissingY.buckets[xIndex];
            if (total > 0) {
                // There could be no data for this specific x value
                if (this.relative)
                    scale = 100 / total;

                let yTotalScaled = 0;
                let yTotal = 0;
                for (let i = 0; i < values.length; i++) {
                    yTotalScaled += values[i] * scale;
                    yTotal += values[i];
                    if (yTotalScaled >= y && !found) {
                        size = significantDigits(values[i]);
                        perc = values[i];
                        value = this.currentData.yData.bucketDescription(i);
                        colorIndex = i;
                        found = true;
                    }
                }
                const missing = this.currentData.heatMap.histogramMissingY.buckets[xIndex];
                yTotal += missing;
                yTotalScaled += missing * scale;
                if (!found && yTotalScaled >= y) {
                    value = "missing";
                    size = significantDigits(missing);
                    perc = missing;
                    colorIndex = -1;
                }
                if (yTotal > 0)
                    perc = perc / yTotal;
            }
            // else value is ""
        }

        const pos = this.cdfPlot.getY(mouseX);
        this.cdfDot.attr("cx", mouseX + this.surface.leftMargin);
        this.cdfDot.attr("cy", (1 - pos) * this.surface.getActualChartHeight() + this.surface.topMargin);
        const cdf = percent(pos);
        this.pointDescription.update([xs, value, ys, size, percent(perc), cdf], mouseX, mouseY);
        this.legendPlot.hilight(colorIndex);
    }

    protected dragCanvasEnd() {
        const dragging = this.dragging && this.moved;
        super.dragEnd();
        if (!dragging)
            return;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x, false);
    }

    // dragging in the legend
   protected dragLegendStart() {
       this.dragging = true;
       this.moved = false;
       const position = d3mouse(this.legendSurface.getCanvas().node());
       this.selectionOrigin = {
           x: position[0],
           y: position[1] };
    }

    protected dragLegendMove(): void {
        if (!this.dragging)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.legendSurface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.legendRect.height();

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
            const delta = this.legendRect.origin.x - ox;
            this.legendSelectionRectangle
                .attr("x", this.legendRect.origin.x)
                .attr("width", width - delta);
        } else if (ox + width > this.legendRect.lowerRight().x) {
            const delta = ox + width - this.legendRect.lowerRight().x;
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

        const position = d3mouse(this.legendSurface.getCanvas().node());
        const x = position[0];
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
        let selectedAxis: AxisData = null;

        if (inLegend) {
            const legendX = this.legendRect.lowerLeft().x;
            xl -= legendX;
            xr -= legendX;
            selectedAxis = this.currentData.yData;
        } else {
            xl -= this.surface.leftMargin;
            xr -= this.surface.leftMargin;
            selectedAxis = this.currentData.xAxisData;
        }

        const x0 = selectedAxis.invertToNumber(xl);
        const x1 = selectedAxis.invertToNumber(xr);

        // selection could be done in reverse
        [min, max] = reorder(x0, x1);
        if (min > max) {
            this.page.reportError("No data selected");
            return;
        }

        const filter: FilterDescription = {
            min: min,
            max: max,
            minString: selectedAxis.invert(xl),
            maxString: selectedAxis.invert(xr),
            cd: selectedAxis.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilterRequest(filter);
        const renderer = new Filter2DReceiver(
            "Filtered on " + selectedAxis.description.name,
            this.currentData.xAxisData.description, this.currentData.yData.description,
            this.schema, inLegend ? this.currentData.xPoints : 0, this.page, rr,
            this.dataset, {
            exact: this.currentData.samplingRate >= 1.0,
            heatmap: false,
            reusePage: false,
            relative: this.relative
        });
        rr.invoke(renderer);
    }

   public static colorMap(d: number): string {
        // The rainbow color map starts and ends with a similar hue
        // so we skip the first 20% of it.
        return d3interpolateRainbow(d * .8 + .2);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        const order =  new RecordOrder([ {
            columnDescription: this.currentData.xAxisData.description,
            isAscending: true,
        }, {
            columnDescription: this.currentData.yData.description,
            isAscending: true,
        } ]);

        const page = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        page.setDataView(table);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }
}

/**
 * Waits for 2 column stats to be received and then
 * initiates a 2D histogram or heatmap rendering.
 */
export class DataRangesCollector extends OnCompleteReceiver<DataRange[]> {
    constructor(
        protected originator: TableTargetAPI,
        page: FullPage,
        operation: ICancellable<DataRange[]>,
        protected schema: SchemaClass,
        protected xBucketCount: number,
        protected cds: IColumnDescription[],  // if 0 we get to choose
        protected title: string | null,
        protected options: ChartOptions) {
        super(page, operation, "histogram");
    }

    public run(ranges: DataRange[]): void {
        console.assert(ranges.length === 2);
        if (ranges[0].presentCount === 0 || ranges[1].presentCount === 0) {
            this.page.reportError("No non-missing data");
            return;
        }
        const rowCount = ranges[0].presentCount + ranges[0].missingCount;

        if (this.options.heatmap) {
            const args: HistogramArgs[] = [];
            const xBucketCount = HistogramViewBase.bucketCount(this.value[0], this.page,
                this.cds[0].kind, true, true);
            const yBucketCount = HistogramViewBase.bucketCount(this.value[1], this.page,
                this.cds[1].kind, true, false);

            let arg = HistogramViewBase.computeHistogramArgs(
                this.cds[0], ranges[0], xBucketCount, this.options.exact, this.page);
            args.push(arg);
            arg = HistogramViewBase.computeHistogramArgs(
                this.cds[1], ranges[1], yBucketCount, this.options.exact, this.page);
            args.push(arg);

            const rr = this.originator.createHeatMapRequest(args);
            const renderer = new HeatMapRenderer(this.page,
                this.originator, rowCount, this.schema,
                this.cds, ranges, 1.0, rr, this.options.reusePage);
            rr.chain(this.operation);
            rr.invoke(renderer);
        } else {
            const args: HistogramArgs[] = [];
            const xBucketCount = this.xBucketCount !== 0 ? this.xBucketCount :
                HistogramViewBase.bucketCount(this.value[0], this.page,
                    this.cds[0].kind, false, true);
            const yBucketCount = HistogramViewBase.bucketCount(this.value[1], this.page,
                this.cds[1].kind, false, false);

            // The first two represent the resolution for the 2D histogram
            const xarg = HistogramViewBase.computeHistogramArgs(
                this.cds[0], ranges[0], xBucketCount, this.options.exact, this.page);
            args.push(xarg);
            const yarg = HistogramViewBase.computeHistogramArgs(
                this.cds[1], ranges[1], yBucketCount, this.options.exact, this.page);
            args.push(yarg);
            // This last one represents the resolution for the CDF
            const cdfArg = HistogramViewBase.computeHistogramArgs(
                this.cds[0], ranges[0], 0, this.options.exact, this.page);
            args.push(cdfArg);

            let samplingRate = HistogramViewBase.samplingRate(
                xBucketCount, rowCount, this.page);
            if (this.options.exact || this.options.relative)
                samplingRate = 1.0;

            const rr = this.originator.createHistogram2DRequest(args);
            const renderer = new Histogram2DRenderer(this.page,
                this.originator, rowCount, this.schema,
                this.cds, ranges, samplingRate, rr,
                this.options.relative, this.options.reusePage);
            rr.chain(this.operation);
            rr.invoke(renderer);
        }
    }
}

/**
 * Receives the result of a filtering operation and initiates
 * a new 2D range computation, which in turns initiates a new 2D histogram or heatmap
 * rendering.
 */
export class Filter2DReceiver extends BaseRenderer {
    constructor(protected title: string,
                protected xColumn: IColumnDescription,
                protected yColumn: IColumnDescription,
                protected schema: SchemaClass,
                protected bucketCount: number,
                page: FullPage,
                operation: ICancellable<RemoteObjectId>,
                dataset: DatasetView,
                protected options: ChartOptions) {
        super(page, operation, "Filter", dataset);
    }

    public run(): void {
        super.run();
        const cds: IColumnDescription[] = [this.xColumn, this.yColumn];
        let buckets;
        if (this.options.heatmap)
            buckets = HistogramViewBase.heatmapSize(this.page);
        else
            buckets = HistogramViewBase.histogram2DSize(this.page);
        const rr = this.remoteObject.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this.remoteObject, this.page, rr, this.schema,
            this.bucketCount, cds, this.title, this.options));
    }
}

/**
 * This class is invoked by the ZipReceiver after a set operation
 * to create a new 2D histogram or heatmap.
 */
export class MakeHistogramOrHeatmap extends BaseRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       private cds: IColumnDescription[],
                       private rowCount: number,
                       private schema: SchemaClass,
                       private options: ChartOptions,
                       dataset: DatasetView) {
        super(page, operation, "Reload", dataset);
    }

    public run(): void {
        super.run();
        let buckets;
        if (this.options.heatmap)
            buckets = HistogramViewBase.heatmapSize(this.page);
        else
            buckets = HistogramViewBase.histogram2DSize(this.page);
        const rr = this.remoteObject.getDataRanges2D(this.cds, buckets);
        rr.invoke(new DataRangesCollector(this.remoteObject, this.page, rr, this.schema, 0,
            this.cds, null, this.options));
    }
}

/**
 * Receives partial results and renders a 2D histogram.
 * The 2D histogram data and the HeatMap data use the same data structure.
 */
export class Histogram2DRenderer extends Receiver<Pair<HeatMap, HistogramBase>> {
    protected histogram: Histogram2DView;

    constructor(page: FullPage,
                protected remoteObject: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected cds: IColumnDescription[],
                protected ranges: DataRange[],
                protected samplingRate: number,
                operation: RpcRequest<PartialResult<Pair<HeatMap, HistogramBase>>>,
                protected relative: boolean,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(Histogram2DRenderer.title(schema, cds), page),
            operation, "histogram");
        this.histogram = new Histogram2DView(
            this.remoteObject.remoteObjectId, rowCount, schema, this.page);
        this.page.setDataView(this.histogram);
        if (cds.length !== 2 || ranges.length !== 2 )
            throw new Error("Expected 2 columns");
    }

    private static title(schema: SchemaClass, cds: IColumnDescription[]): string {
        return "Histogram(" + schema.displayName(cds[0].name) + ", " +
            schema.displayName(cds[1].name) + ")";
    }

    public onNext(value: PartialResult<Pair<HeatMap, HistogramBase>>): void {
        super.onNext(value);
        if (value == null)
            return;
        const heatMap = value.data.first;
        const cdf = value.data.second;
        const points = heatMap.buckets;
        let xPoints = 1;
        let yPoints = 1;
        if (points != null) {
            xPoints = points.length;
            yPoints = points[0] != null ? points[0].length : 1;
        }

        const xAxisData = new AxisData(this.cds[0], this.ranges[0], xPoints);
        const yAxisData = new AxisData(this.cds[1], this.ranges[1], yPoints);
        this.histogram.updateView(heatMap, xAxisData, yAxisData, cdf,
            this.samplingRate, this.relative, this.elapsedMilliseconds());
    }
}

export class Histogram2DDialog extends Dialog {
    public static label(heatmap: boolean): string {
        return heatmap ? "heatmap" : "2D histogram";
    }

    constructor(allColumns: string[], heatmap: boolean) {
        super(Histogram2DDialog.label(heatmap),
            "Display a " + Histogram2DDialog.label(heatmap) + " of the data in two columns");
        this.addSelectField("columnName0", "First Column", allColumns, allColumns[0],
            "First column (X axis)");
        this.addSelectField("columnName1", "Second Column", allColumns, allColumns[1],
            "Second column " + (heatmap ? "(Y axis)" : "(color)"));
    }

    public getColumn(first: boolean): string {
        if (first)
            return this.getFieldValue("columnName0");
        else
            return this.getFieldValue("columnName1");
    }
}

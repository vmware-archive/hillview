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

import {event as d3event, mouse as d3mouse} from "d3-selection";
import {Histogram2DSerialization, IViewSerialization} from "../datasetView";
import {
    AugmentedHistogram,
    FilterDescription,
    Heatmap,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {Receiver, RpcRequest} from "../rpc";
import {DisplayName, SchemaClass} from "../schemaClass";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {CDFPlot} from "../ui/CDFPlot";
import {IDataView} from "../ui/dataview";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {Histogram2DPlot} from "../ui/Histogram2DPlot";
import {HistogramLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {ChartOptions, HtmlString, Rectangle, Resolution} from "../ui/ui";
import {
    formatNumber,
    ICancellable,
    Pair,
    PartialResult,
    percent,
    reorder,
    saveAs, significantDigits,
    significantDigitsHtml,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {FilterReceiver, DataRangesReceiver} from "./dataRangesCollectors";

/**
 * This class is responsible for rendering a 2D histogram.
 * This is a histogram where each bar is divided further into sub-bars.
 */
export class Histogram2DView extends HistogramViewBase {
    protected yData: AxisData;
    protected cdf: AugmentedHistogram;
    protected heatMap: Heatmap;
    protected xPoints: number;
    protected yPoints: number;
    protected relative: boolean;  // true when bars are normalized to 100%
    protected legendRect: Rectangle;  // legend position on the screen; relative to canvas
    protected plot: Histogram2DPlot;
    protected legendPlot: HistogramLegendPlot;
    protected legendSurface: PlottingSurface;

    constructor(remoteObjectId: RemoteObjectId, rowCount: number,
                schema: SchemaClass, protected samplingRate: number, page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "2DHistogram");

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
                help: "Change the number of buckets used for drawing the histogram.",
            }, {
                text: "swap axes",
                action: () => { this.swapAxes(); },
                help: "Redraw this histogram by swapping the X and Y axes.",
            }, {
                text: "heatmap",
                action: () => { this.doHeatmap(); },
                help: "Plot this data as a heatmap view.",
            }, { text: "group by...",
                action: () => {
                    this.trellis();
                },
                help: "Group data by a third column.",
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
        if (this.samplingRate >= 1) {
            const submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
    }

    protected createNewSurfaces(): void {
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        if (this.surface != null)
            this.surface.destroy();
        this.legendSurface = new HtmlPlottingSurface(this.chartDiv, this.page,
            { height: Resolution.legendSpaceHeight });
        this.legendPlot = new HistogramLegendPlot(this.legendSurface,
            (xl, xr) => this.selectionCompleted(xl, xr, true));
        this.surface = new HtmlPlottingSurface(this.chartDiv, this.page, {});
        this.plot = new Histogram2DPlot(this.surface);
        this.cdfPlot = new CDFPlot(this.surface);
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
            case "GAxis":
                return null;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                if (this.relative)
                    return null;
                const range = {
                    min: 0,
                    max: this.plot.maxYAxis != null ? this.plot.maxYAxis : this.plot.max,
                    presentCount: this.rowCount - this.heatMap.missingData,
                    missingCount: this.heatMap.missingData
                };
                return new AxisData(null, range);
        }
        return null;
    }

    public updateView(heatmap: Heatmap, cdf: AugmentedHistogram, maxYAxis: number | null): void {
        this.createNewSurfaces();
        if (heatmap == null || heatmap.buckets.length === 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.xPoints = heatmap.buckets.length;
        this.yPoints = heatmap.buckets[0].length;
        if (this.yPoints === 0) {
            this.page.reportError("No data to display");
            return;
        }
        this.heatMap = heatmap;
        this.cdf = cdf;

        const bucketCount = this.xPoints;
        const canvas = this.surface.getCanvas();

        this.plot.setData(heatmap, this.xAxisData, this.samplingRate, this.relative, this.schema, maxYAxis);
        this.plot.draw();
        const discrete = kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer";

        this.cdfPlot.setData(cdf.cdfBuckets, discrete);
        this.cdfPlot.draw();
        this.legendPlot.setData(this.yData, this.plot.getMissingDisplayed() > 0, this.schema);
        this.legendPlot.draw();

        this.setupMouse();
        this.cdfDot = canvas
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");

        this.legendRect = this.legendPlot.legendRectangle();
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getDisplayNameString(this.schema),
                this.yData.getDisplayNameString(this.schema),
                "y", "count", "%", "cdf"], 40);
        this.pointDescription.show(false);
        let summary = new HtmlString(formatNumber(this.plot.getDisplayedPoints()) + " data points");
        if (heatmap.missingData !== 0)
            summary = summary.appendSafeString(
                ", " + formatNumber(heatmap.missingData) + " missing both coordinates");
        if (heatmap.histogramMissingX.missingData !== 0)
            summary = summary.appendSafeString(
                ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate");
        if (heatmap.histogramMissingY.missingData !== 0)
            summary = summary.appendSafeString(
                ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate");
        summary = summary.appendSafeString(", " + String(bucketCount) + " buckets");
        if (this.samplingRate < 1.0)
            summary = summary.appendSafeString(", sampling rate ")
                .append(significantDigitsHtml(this.samplingRate));
        summary.setInnerHtml(this.summary);
    }

    public serialize(): IViewSerialization {
        const result: Histogram2DSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            relative: this.relative,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yData.description,
            xBucketCount: this.xPoints,
            yBucketCount: this.yPoints
        };
        return result;
    }

    public static reconstruct(ser: Histogram2DSerialization, page: FullPage): IDataView {
        const samplingRate: number = ser.samplingRate;
        const relative: boolean = ser.relative;
        const cd0: IColumnDescription = ser.columnDescription0;
        const cd1: IColumnDescription = ser.columnDescription1;
        const xPoints: number = ser.xBucketCount;
        const yPoints: number = ser.yBucketCount;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd0 === null || cd1 === null || samplingRate === null || schema === null ||
            xPoints === null || yPoints == null)
            return null;

        const hv = new Histogram2DView(ser.remoteObjectId, ser.rowCount, schema, samplingRate, page);
        hv.setAxes(new AxisData(cd0, null), new AxisData(cd1, null), relative);
        hv.xPoints = xPoints;
        hv.yPoints = yPoints;
        return hv;
    }

    public setAxes(xAxisData: AxisData, yData: AxisData, relative: boolean): void {
        this.relative = relative;
        this.xAxisData = xAxisData;
        this.yData = yData;
    }

    public trellis(): void {
        const columns: DisplayName[] = this.schema.displayNamesExcluding(
            [this.xAxisData.description.name, this.yData.description.name]);
        this.chooseTrellis(columns);
    }

    protected showTrellis(colName: DisplayName): void {
        const groupBy = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [
            this.xAxisData.description,
            this.yData.description,
            groupBy];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0, 0], cds, null, {
            reusePage: false, relative: this.relative,
            chartKind: "Trellis2DHistogram", exact: this.samplingRate >= 1.0
        }));
    }

    protected toggleNormalize(): void {
        this.relative = !this.relative;
        if (this.relative && this.samplingRate < 1) {
            // We cannot use sampling when we display relative views.
            this.exactHistogram();
        } else {
            this.refresh();
        }
    }

    public doHeatmap(): void {
        const cds = [this.xAxisData.description, this.yData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false,
            relative: false,
            chartKind: "Heatmap",
            exact: true
        }));
    }

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "histogram2d.csv";
        saveAs(fileName, lines.join("\n"));
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [];
        let line = "";
        for (let y = 0; y < this.yData.bucketCount; y++) {
            const by = this.yData.bucketDescription(y, 0);
            line += "," + JSON.stringify(this.schema.displayName(this.yData.description.name) + " " + by);
        }
        line += ",missing";
        lines.push(line);
        for (let x = 0; x < this.xAxisData.bucketCount; x++) {
            const data = this.heatMap.buckets[x];
            const bx = this.xAxisData.bucketDescription(x, 0);
            let l = JSON.stringify(this.schema.displayName(this.xAxisData.description.name) + " " + bx);
            for (const y of data)
                l += "," + y;
            l += "," + this.heatMap.histogramMissingY.buckets[x];
            lines.push(l);
        }
        line = "mising";
        for (const y of this.heatMap.histogramMissingX.buckets)
            line += "," + y;
        lines.push(line);
        return lines;
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(title, [this.xAxisData.description, this.yData.description],
                this.schema, [0, 0], page, operation, this.dataset, {
                exact: this.samplingRate >= 1, chartKind: "Histogram",
                relative: this.relative, reusePage: false
            });
        };
    }

    public swapAxes(): void {
        if (this == null)
            return;
        const cds = [this.yData.description, this.xAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: true, relative: this.relative,
            chartKind: "2DHistogram", exact: this.samplingRate >= 1.0
        }));
    }

    public exactHistogram(): void {
        if (this == null)
            return;
        const cds = [this.xAxisData.description, this.yData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.xPoints, this.yPoints], cds, this.page.title, {
            reusePage: true,
            relative: this.relative,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    public changeBuckets(bucketCount: number): void {
        const cds = [this.xAxisData.description, this.yData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [bucketCount, this.yPoints], cds, null, {
            reusePage: true,
            relative: this.relative,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.heatMap == null)
            return;

        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange == null)
            return;

        if (eventKind === "XAxis") {
            const collector = new DataRangesReceiver(this,
                this.page, null, this.schema, [0, 0],  // any number of buckets
                [this.xAxisData.description, this.yData.description], this.page.title, {
                    chartKind: "2DHistogram", exact: this.samplingRate >= 1,
                    relative: this.relative, reusePage: true
                });
            collector.run([sourceRange, this.yData.dataRange]);
            collector.finished();
        } else if (eventKind === "YAxis") {
            this.relative = false; // We cannot drag a relative Y axis.
            this.updateView(this.heatMap, this.cdf, sourceRange.max);
        }
    }

    public chooseBuckets(): void {
        if (this == null)
            return;

        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public resize(): void {
        if (this == null)
            return;
        this.updateView(this.heatMap, this.cdf, this.plot.maxYAxis);
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.yData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.xPoints, this.yPoints], cds, null, {
            reusePage: true, relative: this.relative,
            chartKind: "2DHistogram", exact: false
        }));
    }

    public onMouseEnter(): void {
        super.onMouseEnter();
        this.cdfDot.attr("visibility", "visible");
    }

    public onMouseLeave(): void {
        this.cdfDot.attr("visibility", "hidden");
        super.onMouseLeave();
    }

    /**
     * Handles mouse movements in the canvas area only.
     */
    public onMouseMove(): void {
        const position = d3mouse(this.surface.getChart().node());
        // note: this position is within the chart
        const mouseX = position[0];
        const mouseY = position[1];

        const xs = this.xAxisData.invert(position[0]);
        // Use the plot scale, not the yData to invert.  That's the
        // one which is used to draw the axis.
        const y = Math.round(this.plot.getYScale().invert(mouseY));
        let ys = significantDigits(y);
        if (this.relative)
            ys += "%";

        let box = null;
        if (mouseY >= 0 && mouseY < this.surface.getChartHeight())
            box = this.plot.getBoxInfo(mouseX, y);
        const count = (box == null) ? "" : significantDigits(box.count);
        const colorIndex = (box == null) ? null : box.yIndex;
        const value = (box == null) ? "" : this.yData.bucketDescription(colorIndex, 0);
        const perc = (box == null || box.count === 0) ? 0 : box.count / box.countBelow;

        const pos = this.cdfPlot.getY(mouseX);
        this.cdfDot.attr("cx", mouseX + this.surface.leftMargin);
        this.cdfDot.attr("cy", (1 - pos) * this.surface.getChartHeight() + this.surface.topMargin);
        const cdf = percent(pos);
        this.pointDescription.update([xs, value, ys, count, percent(perc), cdf], mouseX, mouseY);
        this.legendPlot.highlight(colorIndex);
    }

    protected dragEnd(): boolean {
        if (!super.dragEnd())
            return false;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x, false);
        return true;
    }

    /**
     * * xl and xr are coordinates of the mouse position within the
     * canvas or legend rectangle respectively.
     */
    protected selectionCompleted(xl: number, xr: number, inLegend: boolean): void {
        let selectedAxis: AxisData = null;
        [xl, xr] = reorder(xl, xr);

        if (inLegend) {
            selectedAxis = this.yData;
        } else {
            xl -= this.surface.leftMargin;
            xr -= this.surface.leftMargin;
            selectedAxis = this.xAxisData;
        }

        const x0 = selectedAxis.invertToNumber(xl);
        const x1 = selectedAxis.invertToNumber(xr);
        if (x0 > x1) {
            this.page.reportError("No data selected");
            return;
        }

        const filter: FilterDescription = {
            min: x0,
            max: x1,
            minString: selectedAxis.invert(xl),
            maxString: selectedAxis.invert(xr),
            cd: selectedAxis.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilterRequest(filter);
        const renderer = new FilterReceiver(
            new PageTitle("Filtered on " + this.schema.displayName(selectedAxis.description.name)),
            [this.xAxisData.description, this.yData.description], this.schema,
            [inLegend ? this.xPoints : 0, this.yPoints], this.page, rr, this.dataset, {
            exact: this.samplingRate >= 1.0,
            chartKind: "2DHistogram",
            reusePage: false,
            relative: this.relative
        });
        rr.invoke(renderer);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true,
        }, {
            columnDescription: this.yData.description,
            isAscending: true,
        } ]);

        const page = this.dataset.newPage(new PageTitle("Table"), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        page.setDataView(table);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }
}

/**
 * Receives partial results and renders a 2D histogram.
 * The 2D histogram data and the Heatmap data use the same data structure.
 */
export class Histogram2DReceiver extends Receiver<Pair<Heatmap, AugmentedHistogram>> {
    protected view: Histogram2DView;

    constructor(title: PageTitle,
                page: FullPage,
                protected remoteObject: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                operation: RpcRequest<PartialResult<Pair<Heatmap, AugmentedHistogram>>>,
                protected options: ChartOptions) {
        super(options.reusePage ? page : page.dataset.newPage(title, page), operation, "histogram");
        this.view = new Histogram2DView(
            this.remoteObject.remoteObjectId, rowCount, schema, samplingRate, this.page);
        this.page.setDataView(this.view);
        this.view.setAxes(axes[0], axes[1], options.relative);
    }

    public onNext(value: PartialResult<Pair<Heatmap, AugmentedHistogram>>): void {
        super.onNext(value);
        if (value == null)
            return;
        const heatmap = value.data.first;
        const cdf = value.data.second;
        this.view.updateView(heatmap, cdf, null);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

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

import { d3 } from "../ui/d3-modules";
import { Renderer } from "../rpc";
import {
    ColumnDescription, Schema, RecordOrder, RangeInfo, Histogram,
    BasicColStats, FilterDescription, HistogramArgs, CombineOperators, RemoteObjectId
} from "../javaBridge";
import {TopMenu, SubMenu} from "../ui/menu";
// noinspection ES6UnusedImports
import {
    Pair, reorder, significantDigits, formatNumber, percent, ICancellable, PartialResult, Seed,
    formatDate, exponentialDistribution
} from "../util";
import {Dialog} from "../ui/dialog";
import {CategoryCache} from "../categoryCache";
import {FullPage} from "../ui/fullPage";
import {Resolution} from "../ui/ui";
import {TextOverlay} from "../ui/textOverlay";
import {AxisData} from "./axisData";
import {HistogramViewBase, BucketDialog} from "./histogramViewBase";
import {Range2DCollector} from "./heatMapView";
import {NextKReceiver, TableView} from "./tableView";
import {RemoteTableObject, RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";
import {HistogramPlot} from "../ui/histogramPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {CDFPlot} from "../ui/CDFPlot";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase {
    protected currentData: {
        histogram: Histogram,
        cdf: Histogram,
        axisData: AxisData,
        samplingRate: number,
        title: string,
    };
    protected menu: TopMenu;
    protected plot: HistogramPlot;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, originalTableId, tableSchema, page);
        this.surface = new PlottingSurface(this.chartDiv, page);
        this.plot = new HistogramPlot(this.surface);
        this.cdfPlot = new CDFPlot(this.surface);

        this.menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view."
                },
                { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying this histogram using a table view."
                },
                { text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this histogram without making any approximations."
                },
                { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw this histogram. " +
                        "The number of buckets must be between 1 and " + Resolution.maxBucketCount
                },
                { text: "correlate...",
                    action: () => this.chooseSecondColumn(),
                    help: "Draw a 2-dimensional histogram using this data and another column."
                },
            ]) },
            {
                text: "Combine", help: "Combine data in two separate views.", subMenu: combineMenu(this, page.pageId)
            }
        ]);

        this.page.setMenu(this.menu);
    }

    public updateView(title: string, cdf: Histogram, h: Histogram,
                      axisData: AxisData, samplingRate: number, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        if (h == null) {
            this.page.reportError("No data to display");
            return;
        }
        if (samplingRate >= 1) {
            let submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
        this.currentData = {
            axisData: axisData,
            title: title,
            cdf: cdf,
            histogram: h,
            samplingRate: samplingRate };

        let counts = h.buckets;
        let bucketCount = counts.length;
        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        this.plot.setHistogram(h, samplingRate, axisData);
        this.plot.draw();
        this.cdfPlot.setData(cdf);
        this.cdfPlot.draw();
        let canvas = this.surface.getCanvas();

        canvas.call(drag)
            .on("mousemove", () => this.mouseMove())
            .on("mouseenter", () => this.mouseEnter())
            .on("mouseleave", () => this.mouseLeave());

        this.cdfDot = canvas
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");

        this.selectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);

        let pointDesc = ["x", "y"];
        if (cdf != null)
            pointDesc.push("cdf");
        this.pointDescription = new TextOverlay(this.surface.getChart(), pointDesc, 40);
        this.pointDescription.show(false);

        let summary = "";
        if (h.missingData != 0)
            summary = formatNumber(h.missingData) + " missing, ";
        summary += formatNumber(axisData.stats.presentCount + axisData.stats.missingCount) + " points";
        if (axisData.distinctStrings != null)
            summary += ", " + (axisData.stats.max - axisData.stats.min) + " distinct values";
        summary += ", " + String(bucketCount) + " buckets";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
    }

    // combine two views according to some operation
    combine(how: CombineOperators): void {
        let r = SelectedObject.instance.getSelected(this, this.getPage().getErrorReporter());
        if (r == null)
            return;

        let title = "[" + SelectedObject.instance.getPage() + "] " + CombineOperators[how];
        let rr = this.createZipRequest(r);
        let finalRenderer = (page: FullPage, operation: ICancellable) => {
            return new MakeHistogram(title, page, operation, this.currentData.axisData.description,
                this.tableSchema, this.currentData.samplingRate, this.currentData.axisData.distinctStrings,
                this.originalTableId);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.originalTableId, finalRenderer));
    }

    chooseSecondColumn(): void {
        let columns : string[] = [];
        for (let i = 0; i < this.tableSchema.length; i++) {
            let col = this.tableSchema[i];
            if (col.kind == "String" || col.kind == "Json")
                continue;
            if (col.name == this.currentData.axisData.description.name)
                continue;
            columns.push(col.name);
        }
        if (columns.length == 0) {
            this.page.reportError("No other acceptable columns found");
            return;
        }

        let dialog = new Dialog("Choose column", "Select a second column to use for displaying a 2D histogram.");
        dialog.addSelectField("column", "column", columns, null,
            "The second column that will be used in addition to the one displayed here " +
            "for drawing a two-dimensional histogram.");
        dialog.setAction(() => this.showSecondColumn(dialog.getFieldValue("column")));
        dialog.show();
    }

    private showSecondColumn(colName: string) {
        let oc = TableView.findColumn(this.tableSchema, colName);
        let cds: ColumnDescription[] = [this.currentData.axisData.description, oc];
        let catColumns: string[] = [];
        if (oc.kind == "Category")
            catColumns.push(colName);

        let cont = (operation: ICancellable) => {
            let r0 = this.currentData.axisData.getRangeInfo();
            let ds = CategoryCache.instance.getDistinctStrings(this.originalTableId, colName);
            let r1 = new RangeInfo(colName, ds != null ? ds.uniqueStrings : null);
            let distinct: DistinctStrings[] = [this.currentData.axisData.distinctStrings, ds];
            let rr = this.createRange2DRequest(r0, r1);
            rr.chain(operation);
            rr.invoke(new Range2DCollector(cds, this.tableSchema, distinct, this.getPage(), this,
                this.currentData.samplingRate >=1, rr, false));
        };
        CategoryCache.instance.retrieveCategoryValues(this, catColumns, this.getPage(), cont);
    }

    changeBuckets(bucketCount: number): void {
        if (bucketCount == null)
            return;
        let cdfBucketCount = Math.floor(this.currentData.cdf.buckets.length);
        let col = this.currentData.axisData.getColumnAndRange(cdfBucketCount);
        let samplingRate = HistogramViewBase.samplingRate(
            bucketCount, this.currentData.axisData.stats.presentCount, this.page);
        let histoArg: HistogramArgs = {
            column: col,
            seed: Seed.instance.get(),
            bucketCount: +bucketCount,
            samplingRate: samplingRate,
            cdfBucketCount: cdfBucketCount,
            cdfSamplingRate: this.currentData.samplingRate,
        };
        let rr = this.createHistogramRequest(histoArg);
        let axisData = new AxisData(this.currentData.axisData.description,
            this.currentData.axisData.stats, this.currentData.axisData.distinctStrings,
            +bucketCount);
        let renderer = new HistogramRenderer(this.currentData.title, this.page,
            this.remoteObjectId, this.tableSchema, axisData, rr, this.currentData.samplingRate,
            this.originalTableId);
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
            this.currentData.title,
            this.currentData.cdf,
            this.currentData.histogram,
            this.currentData.axisData,
            this.currentData.samplingRate,
            0);
    }

    exactHistogram(): void {
        if (this.currentData == null)
            return;
        let rc = new RangeCollector(this.currentData.title, this.currentData.axisData.description,
            this.tableSchema, this.currentData.axisData.distinctStrings, this.page, this, true, null);
        rc.setValue(this.currentData.axisData.stats);
        rc.onCompleted();
    }

    public mouseMove(): void {
        let position = d3.mouse(this.surface.getChart().node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = "";
        if (this.plot.xScale != null) {
            xs = HistogramViewBase.invert(
                position[0], this.plot.xScale, this.currentData.axisData.description.kind, this.currentData.axisData.distinctStrings)
        }
        let y = Math.round(this.plot.yScale.invert(position[1]));
        let ys = significantDigits(y);
        let mouseLabel = [xs, ys];

        if (this.cdfPlot != null) {
            let pos = this.cdfPlot.getY(mouseX);
            this.cdfDot.attr("cx", mouseX + this.surface.leftMargin);
            this.cdfDot.attr("cy", (1 - pos) *
                this.surface.getActualChartHeight() + this.surface.topMargin);
            let perc = percent(pos);
            mouseLabel.push(perc);
        }
        this.pointDescription.update(mouseLabel, mouseX, mouseY);
    }

    // override
    public dragMove() {
        this.mouseMove();
        super.dragMove();
    }

    public dragEnd() {
        let dragging = this.dragging && this.moved;
        super.dragEnd();
        if (!dragging)
            return;
        let position = d3.mouse(this.surface.getCanvas().node());
        let x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        let newPage = new FullPage("Table view", "Table", this.page);
        let table = new TableView(this.remoteObjectId, this.originalTableId, newPage);
        newPage.setDataView(table);
        this.page.insertAfterMe(newPage);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.currentData.axisData.description,
            isAscending: true
        } ]);
        let rr = table.createNextKRequest(order, null);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order));
    }

    /**
     * Selection has been completed.
     * @param {number} xl: X mouse coordinate within canvas.
     * @param {number} xr: Y mouse coordinate within canvas.
     */
    protected selectionCompleted(xl: number, xr: number): void {
        if (this.plot == null || this.plot.xScale == null)
            return;

        // coordinates within chart
        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;

        let kind = this.currentData.axisData.description.kind;
        let x0 = HistogramViewBase.invertToNumber(xl, this.plot.xScale, kind);
        let x1 = HistogramViewBase.invertToNumber(xr, this.plot.xScale, kind);

        // selection could be done in reverse
        let min: number;
        let max: number;
        [min, max] = reorder(x0, x1);
        if (min > max) {
            this.page.reportError("No data selected");
            return;
        }

        let boundaries: string[] = null;
        if (this.currentData.axisData.distinctStrings != null)
            boundaries = this.currentData.axisData.distinctStrings.categoriesInRange(
                min, max, this.currentData.cdf.buckets.length);
        let filter: FilterDescription = {
            min: min,
            max: max,
            kind: this.currentData.axisData.description.kind,
            columnName: this.currentData.axisData.description.name,
            complement: d3.event.sourceEvent.ctrlKey,
            bucketBoundaries: boundaries
        };

        let rr = this.createFilterRequest(filter);
        let renderer = new FilterReceiver(
            filter, this.currentData.axisData.description, this.tableSchema,
            this.currentData.axisData.distinctStrings, this.currentData.samplingRate >= 1.0,
            this.page, rr, this.originalTableId);
        rr.invoke(renderer);
    }
}

/**
 * Receives the results of a filtering operation and initiates a new Range computation
 * (which in turn will initiate a new histogram rendering).
 */
class FilterReceiver extends RemoteTableRenderer {
    constructor(
        protected filter: FilterDescription,
        protected columnDescription: ColumnDescription,
        protected tableSchema: Schema,
        protected allStrings: DistinctStrings,
        protected exact: boolean,
        page: FullPage,
        operation: ICancellable,
        originalTableId: RemoteObjectId) {
        super(page, operation, "Filter", originalTableId);
    }

    private filterDescription() {
        return "Filtered";  // TODO: add title description
    }

    public run(): void {
        super.run();
        let colName = this.columnDescription.name;
        let rangeInfo: RangeInfo = new RangeInfo(colName);
        if (this.allStrings != null)
            rangeInfo = this.allStrings.getRangeInfo(colName);
        let rr = this.remoteObject.createRangeRequest(rangeInfo);
        rr.chain(this.operation);
        let title = this.filterDescription();
        rr.invoke(new RangeCollector(title, this.columnDescription, this.tableSchema,
                  this.allStrings, this.page, this.remoteObject, this.exact, this.operation));
    }
}

/**
 * Waits for column stats to be received and then initiates a histogram rendering.
  */
export class RangeCollector extends Renderer<BasicColStats> {
    protected stats: BasicColStats;
    constructor(protected title: string,  // title of the resulting display
                protected cd: ColumnDescription,
                protected tableSchema: Schema,
                protected allStrings: DistinctStrings,  // for categorical columns only
                page: FullPage,
                protected remoteObject: RemoteTableObject,
                protected exact: boolean,  // if true we should do no sampling
                operation: ICancellable) {
        super(page, operation, "histogram");
    }

    public setValue(bcs: BasicColStats): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: RemoteTableObject) {
        this.remoteObject = ro;
    }

    onNext(value: PartialResult<BasicColStats>): void {
        super.onNext(value);
        this.setValue(value.data);
        HistogramViewBase.adjustStats(this.cd.kind, this.stats);
    }

    public histogram(): void {
        let size = PlottingSurface.getDefaultChartSize(this.page);
        let cdfCount = Math.floor(size.width);
        let bucketCount = HistogramViewBase.bucketCount(this.stats, this.page, this.cd.kind, false, true);
        if (cdfCount == 0)
            cdfCount = bucketCount;

        let samplingRate = 1.0;
        if (!this.exact)
            samplingRate = HistogramViewBase.samplingRate(bucketCount, this.stats.presentCount, this.page);

        let column = HistogramViewBase.getRange(this.stats, this.cd, this.allStrings, cdfCount);
        let args: HistogramArgs = {
            column: column,
            samplingRate: samplingRate,
            cdfSamplingRate: samplingRate,
            seed: Seed.instance.get(),
            cdfBucketCount: cdfCount,
            bucketCount: bucketCount
        };
        let rr = this.remoteObject.createHistogramRequest(args);

        rr.chain(this.operation);
        let axisData = new AxisData(this.cd, this.stats, this.allStrings, bucketCount);
        let renderer = new HistogramRenderer(this.title, this.page,
            this.remoteObject.remoteObjectId, this.tableSchema,
            axisData, rr, samplingRate, this.remoteObject.originalTableId);
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

/**
 * Renders a 1D histogram.
  */
export class HistogramRenderer extends Renderer<Pair<Histogram, Histogram>> {
    protected histogram: HistogramView;
    protected timeInMs: number;
    /*
    // The following field is only used for measurements.
    // If set to true then the renderer automatically triggers another histogram
    // after a think time.
    protected infiniteLoop: boolean = false;
    */

    constructor(protected title: string,
                sourcePage: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected axisData: AxisData,
                operation: ICancellable,
                protected samplingRate: number,
                originalTableId: RemoteObjectId) {
        super(new FullPage(title, "Histogram", sourcePage), operation, "histogram");
        sourcePage.insertAfterMe(this.page);
        this.histogram = new HistogramView(remoteTableId, originalTableId, schema, this.page);
        this.page.setDataView(this.histogram);
    }

    onNext(value: PartialResult<Pair<Histogram, Histogram>>): void {
        super.onNext(value);
        this.timeInMs = this.elapsedMilliseconds();
        this.histogram.updateView(this.title, value.data.first, value.data.second,
            this.axisData, this.samplingRate, this.timeInMs);
    }

    /*
    onCompleted(): void {
        super.onCompleted();
        if (!this.infiniteLoop)
            return;
        // The following code is just for running experiments.
        // It keeps initiating histograms in an infinite loop
        // with a Poisson process between initiations.
        let arrivalRate = .2;  // mean is 5 seconds
        let time = exponentialDistribution(arrivalRate);
        HistogramRenderer.lastMeasurements.push(this.timeInMs);
        if (HistogramRenderer.lastMeasurements.length == 5)
            HistogramRenderer.lastMeasurements.splice(0, 1);
        let min = 1e6, max = 0;
        for (let i=0; i < HistogramRenderer.lastMeasurements.length; i++) {
            let val = HistogramRenderer.lastMeasurements[i];
            if (val < min)
                min = val;
            if (val > max)
                max = val;
        }
        this.page.reportError("Latency range=[" + significantDigits(min) +
            ".." + significantDigits(max) + "]");

        //this.page.reportError("Starting a new histogram in " + time + "s");
        setTimeout(() => this.histogramInInfiniteLoop(), time * 1000);
    }

    // A stack of two prior pages; we can't close the current page,
    // because the next one is inserted after it.
    private static pageToClose: FullPage = null;
    private static nextPage: FullPage = null;
    private static lastMeasurements: number[] = [];

    histogramInInfiniteLoop(): void {
        if (this.histogram == null)
            return;

        // This stuff only works if there is exactly 1 histogram running at a time.
        if (HistogramRenderer.pageToClose != null)
            HistogramRenderer.pageToClose.remove();
        HistogramRenderer.pageToClose = HistogramRenderer.nextPage;
        HistogramRenderer.nextPage = this.page;

        let rangeInfo: RangeInfo;
        if (this.allStrings != null)
            rangeInfo = this.allStrings.getRangeInfo(this.cd.name);
        else
            rangeInfo = new RangeInfo(this.cd.name);
        let rr = this.histogram.createRangeRequest(rangeInfo);
        rr.invoke(new RangeCollector(this.cd, this.schema, this.allStrings, this.page, this.histogram, false, rr));
    }
    */
}

/**
 * This class is invoked by the ZipReceiver after a set operation to create a new histogram
  */
class MakeHistogram extends RemoteTableRenderer {
    public constructor(private title: string,
                       page: FullPage,
                       operation: ICancellable,
                       private colDesc: ColumnDescription,
                       private schema: Schema,
                       protected samplingRate: number,
                       private allStrings: DistinctStrings,
                       originalTableId: RemoteObjectId) {
        super(page, operation, "Reload", originalTableId);
    }

    run(): void {
        super.run();
        if (this.colDesc.kind == "Category") {
            // Continuation invoked after the distinct strings have been obtained
            let cont = (operation: ICancellable) => {
                let ds = CategoryCache.instance.getDistinctStrings(this.originalTableId, this.colDesc.name);
                if (ds == null)
                // Probably an error has occurred
                    return;
                let rr = this.remoteObject.createRangeRequest(ds.getRangeInfo(this.colDesc.name));
                rr.chain(operation);
                rr.invoke(new RangeCollector(this.title, this.colDesc, this.schema, ds,
                    this.page, this.remoteObject, this.samplingRate >= 1, rr));
            };
            // Get the categorical data and invoke the continuation
            CategoryCache.instance.retrieveCategoryValues(this.remoteObject, [this.colDesc.name], this.page, cont);
        } else {
            let rr = this.remoteObject.createRangeRequest(
                {columnName: this.colDesc.name, allNames: null, seed: Seed.instance.get()});
            rr.chain(this.operation);
            rr.invoke(new RangeCollector(this.title, this.colDesc, this.schema, this.allStrings,
                this.page, this.remoteObject, this.samplingRate >= 1, rr));
        }
    }
}

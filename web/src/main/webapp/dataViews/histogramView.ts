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
    BasicColStats, FilterDescription, HistogramArgs, CombineOperators
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
import {TableRenderer, TableView} from "./tableView";
import {RemoteTableObject, RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase {
    protected currentData: {
        histogram: Histogram,
        cdf: Histogram,
        cdfSum: number[],  // prefix sum of cdf
        axisData: AxisData,
        samplingRate: number,
        title: string,
    };
    protected menu: TopMenu;

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, tableSchema, page);
        this.menu = new TopMenu( [
            { text: "View", subMenu: new SubMenu([
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "table", action: () => this.showTable() },
                { text: "exact", action: () => this.exactHistogram() },
                { text: "# buckets...", action: () => this.chooseBuckets() },
                { text: "correlate...", action: () => this.chooseSecondColumn() },
            ]) },
            {
                text: "Combine", subMenu: combineMenu(this, page.pageId)
            }
        ]);

        this.page.setMenu(this.menu);
    }

    // combine two views according to some operation
    combine(how: CombineOperators): void {
        let r = SelectedObject.current.getSelected();
        if (r == null) {
            this.page.reportError("No view selected");
            return;
        }

        let title = "[" + SelectedObject.current.getPage() + "] " + CombineOperators[how];
        let rr = this.createZipRequest(r);
        let finalRenderer = (page: FullPage, operation: ICancellable) => {
            return new MakeHistogram(title, page, operation, this.currentData.axisData.description,
                this.tableSchema, this.currentData.samplingRate, this.currentData.axisData.distinctStrings);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, finalRenderer));
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

        let dialog = new Dialog("Choose column");
        dialog.addSelectField("column", "column", columns);
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
            let ds = CategoryCache.instance.getDistinctStrings(colName);
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
        let axisData = new AxisData(null, this.currentData.axisData.description,
            this.currentData.axisData.stats, this.currentData.axisData.distinctStrings,
            +bucketCount);
        let renderer = new HistogramRenderer(this.currentData.title, this.page,
            this.remoteObjectId, this.tableSchema, axisData, rr, this.currentData.samplingRate);
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

    protected onMouseMove(): void {
        let position = d3.mouse(this.chart.node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = "";
        if (this.xScale != null) {
            xs = HistogramViewBase.invert(
                position[0], this.xScale, this.currentData.axisData.description.kind, this.currentData.axisData.distinctStrings)
        }
        let y = Math.round(this.yScale.invert(position[1]));
        let ys = significantDigits(y);
        let mouseLabel = [xs, ys];

        if (this.currentData.cdfSum != null) {
            // determine mouse position on cdf curve
            // we have to take into account the adjustment
            let cdfX = mouseX * this.currentData.cdfSum.length / this.chartSize.width;
            let pos = 0;
            if (cdfX < 0) {
                pos = 0;
            } else if (cdfX >= this.currentData.cdfSum.length) {
                pos = 1;
            } else {
                let cdfPosition = this.currentData.cdfSum[Math.floor(cdfX)];
                pos = cdfPosition / this.currentData.axisData.stats.presentCount;
            }

            this.cdfDot.attr("cx", mouseX + Resolution.leftMargin);
            this.cdfDot.attr("cy", (1 - pos) * this.chartSize.height + Resolution.topMargin);
            let perc = percent(pos);
            mouseLabel.push(perc);
        }
        this.pointDescription.update(mouseLabel, mouseX, mouseY);
    }

    public updateView(title: string, cdf: Histogram, h: Histogram,
                      axisData: AxisData, samplingRate: number, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
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
            cdfSum: null,
            histogram: h,
            samplingRate: samplingRate };

        let canvasSize = Resolution.getCanvasSize(this.page);
        this.chartSize = Resolution.getChartSize(this.page);

        let counts = h.buckets;
        let bucketCount = counts.length;
        let max = Math.max(...counts);

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
                point = this.currentData.cdfSum[i] * max / axisData.stats.presentCount;
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
        let canvasHeight = canvasSize.height;
        this.canvas = d3.select(this.chartDiv)
            .append("svg")
            .attr("id", "canvas")
            .call(drag)
            .attr("width", canvasSize.width)
            .attr("border", 1)
            .attr("height", canvasHeight)
            .attr("cursor", "crosshair");

        this.canvas.on("mousemove", () => this.onMouseMove());

        // The chart uses a fragment of the canvas offset by the margins
        this.chart = this.canvas
            .append("g")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`);

        this.yScale = d3.scaleLinear()
            .domain([0, max])
            .range([this.chartSize.height, 0]);
        let yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let scaleAxis = axisData.scaleAndAxis(this.chartSize.width, true);
        this.xScale = scaleAxis.scale;
        let xAxis = scaleAxis.axis;

        // After resizing the line may not have the exact number of points
        // as the screen width.
        let cdfLine = d3.line<number>()
            .x((d, i) => {
                let index = Math.floor(i / 2); // two points for each data point, for a zig-zag
                return index * 2 * this.chartSize.width / cdfData.length;
            })
            .y(d => this.yScale(d));

        // draw CDF curve
        this.canvas.append("path")
            .attr("transform", `translate(${Resolution.leftMargin}, ${Resolution.topMargin})`)
            .datum(cdfData)
            .attr("stroke", "blue")
            .attr("d", cdfLine)
            .attr("fill", "none");

        let barWidth = this.chartSize.width / bucketCount;
        let bars = this.chart.selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => `translate(${i * barWidth}, 0)`);

        bars.append("rect")
            .attr("y", d => this.yScale(d))
            .attr("fill", "darkcyan")
            .attr("height", d => this.chartSize.height - this.yScale(d))
            .attr("width", barWidth - 1);

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", barWidth / 2)
            .attr("y", d => this.yScale(d))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text(d => HistogramViewBase.boxHeight(d, this.currentData.samplingRate,
                this.currentData.axisData.stats.presentCount))
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
        this.selectionRectangle = this.canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);

        let pointDesc = ["x", "y"];
        if (this.currentData.cdfSum != null)
            pointDesc.push("cdf");
        this.pointDescription = new TextOverlay(this.chart, pointDesc, 40);

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

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        let newPage = new FullPage("Table view", "Table", this.page);
        let table = new TableView(this.remoteObjectId, newPage);
        newPage.setDataView(table);
        this.page.insertAfterMe(newPage);
        table.setSchema(this.tableSchema);

        let order =  new RecordOrder([ {
            columnDescription: this.currentData.axisData.description,
            isAscending: true
        } ]);
        let rr = table.createNextKRequest(order, null);
        rr.invoke(new TableRenderer(newPage, table, rr, false, order));
    }

    protected selectionCompleted(xl: number, xr: number): void {
        if (this.xScale == null)
            return;

        let kind = this.currentData.axisData.description.kind;
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
            this.page, rr);
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
        operation: ICancellable) {
        super(page, operation, "Filter");
    }

    private filterDescription() {
        return "Filtered";  // TODO: add title description
    }

    public onCompleted(): void {
        this.finished();
        if (this.remoteObject == null)
            return;

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
        let size = Resolution.getChartSize(this.page);
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
        let axisData = new AxisData(null, this.cd, this.stats, this.allStrings, bucketCount);
        let renderer = new HistogramRenderer(this.title, this.page,
            this.remoteObject.remoteObjectId, this.tableSchema,
            axisData, rr, samplingRate);
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
                page: FullPage,
                remoteTableId: string,
                protected schema: Schema,
                protected axisData: AxisData,
                operation: ICancellable,
                protected samplingRate: number) {
        super(new FullPage(title, "Histogram", page), operation, "histogram");
        page.insertAfterMe(this.page);
        this.histogram = new HistogramView(remoteTableId, schema, this.page);
        this.page.setDataView(this.histogram);
    }

    onNext(value: PartialResult<Pair<Histogram, Histogram>>): void {
        super.onNext(value);
        this.timeInMs = this.elapsedMilliseconds();
        this.histogram.updateView(this.title, value.data.first, value.data.second,
            this.axisData, this.samplingRate, this.timeInMs);
        this.histogram.scrollIntoView();
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
                       private allStrings: DistinctStrings) {
        super(page, operation, "Reload");
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteObject == null)
            return;
        if (this.colDesc.kind == "Category") {
            // Continuation invoked after the distinct strings have been obtained
            let cont = (operation: ICancellable) => {
                let ds = CategoryCache.instance.getDistinctStrings(this.colDesc.name);
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

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
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {DatasetView, HistogramSerialization, IViewSerialization} from "../datasetView";
import {DistinctStrings} from "../distinctStrings";
import {BasicColStats, CategoricalValues, CombineOperators, FilterDescription,
    Histogram, HistogramArgs, IColumnDescription, RecordOrder, RemoteObjectId,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, TableTargetAPI, ZipReceiver} from "../tableTarget";
import {CDFPlot} from "../ui/CDFPlot";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {Resolution} from "../ui/ui";
import {
    formatNumber, ICancellable, Pair, PartialResult, percent, reorder, saveAs, Seed,
    significantDigits,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {ChartObserver} from "./tsViewBase";

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

    constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Histogram");
        this.surface = new PlottingSurface(this.chartDiv, page);
        this.plot = new HistogramPlot(this.surface);
        this.cdfPlot = new CDFPlot(this.surface);

        this.menu = new TopMenu( [{
            text: "Export",
            help: "Save the information in this view in a local file.",
            subMenu: new SubMenu([{
                text: "As CSV",
                help: "Saves the data in this view in a CSV file.",
                action: () => { this.export(); },
            }]),
            }, { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                },
                { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying this histogram using a table view.",
                },
                { text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this histogram without making any approximations.",
                },
                { text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw this histogram. " +
                        "The number of buckets must be between 1 and " + Resolution.maxBucketCount,
                },
                { text: "correlate...",
                    action: () => this.chooseSecondColumn(),
                    help: "Draw a 2-dimensional histogram using this data and another column.",
                },
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
    }

    public serialize(): IViewSerialization {
        const result: HistogramSerialization = {
            ...super.serialize(),
            exact: this.currentData.samplingRate >= 1,
            columnDescription: this.currentData.axisData.description,
        };
        return result;
    }

    public static reconstruct(ser: HistogramSerialization, page: FullPage): IDataView {
        const exact: boolean = ser.exact;
        const cd: IColumnDescription = ser.columnDescription;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd == null || exact == null || schema == null)
            return null;

        const hv = new HistogramView(ser.remoteObjectId, ser.rowCount, schema, page);
        const rr = page.dataset.createGetCategoryRequest(page, [cd]);
        rr.invoke(new ChartObserver(hv, page, rr, null,
            ser.rowCount, schema,
            { exact, heatmap: false, relative: false, reusePage: true }, [cd]));
        return hv;
    }

    public updateView(title: string, cdf: Histogram, h: Histogram,
                      axisData: AxisData, samplingRate: number, elapsedMs: number): void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        if (h == null) {
            this.page.reportError("No data to display");
            return;
        }
        if (samplingRate >= 1) {
            const submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
        this.currentData = {
            axisData,
            title,
            cdf,
            histogram: h,
            samplingRate };

        const counts = h.buckets;
        const bucketCount = counts.length;
        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        this.plot.setHistogram(h, samplingRate, axisData);
        this.plot.draw();
        this.cdfPlot.setData(cdf);
        this.cdfPlot.draw();
        const canvas = this.surface.getCanvas();

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

        const pointDesc = ["x", "y", "size"];
        if (cdf != null)
            pointDesc.push("cdf");
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getDefaultChartSize(), pointDesc, 40);
        this.pointDescription.show(false);

        let summary = "";
        if (h.missingData !== 0)
            summary = formatNumber(h.missingData) + " missing, ";
        summary += formatNumber(axisData.stats.presentCount + axisData.stats.missingCount) + " points";
        if (axisData.distinctStrings != null && axisData.distinctStrings.uniqueStrings != null)
            summary += ", " + (axisData.stats.max - axisData.stats.min) + " distinct values";
        summary += ", " + String(bucketCount) + " buckets";
        if (samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(samplingRate);
        this.summary.innerHTML = summary;
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        const r = this.dataset.getSelected();
        if (r.first == null)
            return;

        const title = "[" + r.second + "] " + CombineOperators[how];
        const rr = this.createZipRequest(r.first);
        const finalRenderer = (page: FullPage, operation: ICancellable) => {
            return new MakeHistogram(
                title, page, operation, this.currentData.axisData.description,
                this.rowCount, this.schema, this.currentData.samplingRate,
                this.dataset);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.dataset, finalRenderer));
    }

    public chooseSecondColumn(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.kind === "String" || col.kind === "Json")
                continue;
            if (col.name === this.currentData.axisData.description.name)
                continue;
            columns.push(col.name);
        }
        if (columns.length === 0) {
            this.page.reportError("No other acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column", "Select a second column to use for displaying a 2D histogram.");
        dialog.addSelectField("column", "column", columns, null,
            "The second column that will be used in addition to the one displayed here " +
            "for drawing a two-dimensional histogram.");
        dialog.setAction(() => this.showSecondColumn(dialog.getFieldValue("column")));
        dialog.show();
    }

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "histogram.csv";
        saveAs(fileName, lines.join("\n"));
        this.page.reportError("Check the downloads folder for a file named '" + fileName + "'");
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [];
        let line = this.currentData.axisData.description.name + ",count";
        lines.push(line);
        for (let x = 0; x < this.currentData.histogram.buckets.length; x++) {
            const bx = this.currentData.axisData.bucketDescription(x);
            const l = "" + JSON.stringify(bx) + "," + this.currentData.histogram.buckets[x];
            lines.push(l);
        }
        line = "missing," + this.currentData.histogram.missingData;
        lines.push(line);
        return lines;
    }

    private showSecondColumn(colName: string) {
        const oc = this.schema.find(colName);
        const cds: IColumnDescription[] = [this.currentData.axisData.description, oc];
        const rr = this.dataset.createGetCategoryRequest(this.page, cds);
        rr.invoke(new ChartObserver(this, this.page, rr, null,
            this.rowCount, this.schema,
            { exact: this.currentData.samplingRate >= 1,
                heatmap: false, relative: false, reusePage: false }, cds));
    }

    public changeBuckets(bucketCount: number): void {
        if (bucketCount == null)
            return;
        const cdfBucketCount = Math.floor(this.currentData.cdf.buckets.length);
        const col = this.currentData.axisData.getColumnAndRange(cdfBucketCount);
        const samplingRate = HistogramViewBase.samplingRate(
            bucketCount, this.currentData.axisData.stats.presentCount, this.page);
        const histoArg: HistogramArgs = {
            column: col,
            seed: Seed.instance.get(),
            bucketCount: +bucketCount,
            samplingRate,
            cdfBucketCount,
            cdfSamplingRate: this.currentData.samplingRate,
        };
        const rr = this.createHistogramRequest(histoArg);
        const axisData = new AxisData(this.currentData.axisData.description,
            this.currentData.axisData.stats, this.currentData.axisData.distinctStrings,
            +bucketCount);
        const renderer = new HistogramRenderer(this.currentData.title, this.page,
            this.remoteObjectId, this.rowCount, this.schema, axisData, rr, this.currentData.samplingRate,
            true);
        rr.invoke(renderer);
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
            this.currentData.title,
            this.currentData.cdf,
            this.currentData.histogram,
            this.currentData.axisData,
            this.currentData.samplingRate,
            0);
    }

    public exactHistogram(): void {
        if (this.currentData == null)
            return;
        const rc = new RangeCollector(
            this.currentData.title, this.currentData.axisData.description,
            this.rowCount, this.schema, this.currentData.axisData.distinctStrings,
            this.page, this, true, null, true);
        rc.setValue(this.currentData.axisData.stats);
        rc.onCompleted();
    }

    public mouseMove(): void {
        const position = d3mouse(this.surface.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];

        let xs = "";
        if (this.plot.xScale != null) {
            xs = HistogramViewBase.invert(
                position[0], this.plot.xScale, this.currentData.axisData.description.kind,
                this.currentData.axisData.distinctStrings);
        }
        const y = Math.round(this.plot.yScale.invert(position[1]));
        const ys = formatNumber(y);
        const size = this.plot.get(mouseX);
        const pointDesc = [xs, ys, formatNumber(size)];

        if (this.cdfPlot != null) {
            const pos = this.cdfPlot.getY(mouseX);
            this.cdfDot.attr("cx", mouseX + this.surface.leftMargin);
            this.cdfDot.attr("cy", (1 - pos) *
                this.surface.getActualChartHeight() + this.surface.topMargin);
            const perc = percent(pos);
            pointDesc.push(perc);
        }
        this.pointDescription.update(pointDesc, mouseX, mouseY);
    }

    // override
    public dragMove() {
        this.mouseMove();
        super.dragMove();
    }

    public dragEnd() {
        const dragging = this.dragging && this.moved;
        super.dragEnd();
        if (!dragging)
            return;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        const newPage = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.currentData.axisData.description,
            isAscending: true,
        } ]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
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

        const kind = this.currentData.axisData.description.kind;
        const x0 = HistogramViewBase.invertToNumber(xl, this.plot.xScale, kind);
        const x1 = HistogramViewBase.invertToNumber(xr, this.plot.xScale, kind);

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
        const filter: FilterDescription = {
            min: min,
            max: max,
            kind: this.currentData.axisData.description.kind,
            columnName: this.currentData.axisData.description.name,
            complement: d3event.sourceEvent.ctrlKey,
            bucketBoundaries: boundaries,
        };

        const rr = this.createFilterRequest(filter);
        const renderer = new FilterReceiver(
            filter, this.currentData.axisData.description, this.rowCount, this.schema,
            this.currentData.axisData.distinctStrings, this.currentData.samplingRate >= 1.0,
            this.page, rr, this.dataset);
        rr.invoke(renderer);
    }
}

/**
 * Receives the results of a filtering operation and initiates a new Range computation
 * (which in turn will initiate a new histogram rendering).
 */
class FilterReceiver extends BaseRenderer {
    constructor(
        protected filter: FilterDescription,
        protected columnDescription: IColumnDescription,
        protected rowCount: number,
        protected schema: SchemaClass,
        protected allStrings: DistinctStrings,
        protected exact: boolean,
        page: FullPage,
        operation: ICancellable,
        dataset: DatasetView) {
        super(page, operation, "Filter", dataset);
    }

    private filterDescription() {
        return "Filtered";  // TODO: add title description
    }

    public run(): void {
        super.run();
        const colName = this.columnDescription.name;
        let catValues: CategoricalValues = new CategoricalValues(colName);
        if (this.allStrings != null)
            catValues = this.allStrings.getCategoricalValues();
        const rr = this.remoteObject.createRangeRequest(catValues);
        rr.chain(this.operation);
        const title = this.filterDescription();
        rr.invoke(
            new RangeCollector(title, this.columnDescription, this.rowCount, this.schema,
                  this.allStrings, this.page, this.remoteObject, this.exact, this.operation, false));
    }
}

/**
 * Waits for column stats to be received and then initiates a histogram rendering.
 */
export class RangeCollector extends Receiver<BasicColStats> {
    protected stats: BasicColStats;
    constructor(protected title: string,  // title of the resulting display
                protected cd: IColumnDescription,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected allStrings: DistinctStrings,  // for categorical columns only
                page: FullPage,
                protected remoteObject: TableTargetAPI,
                protected exact: boolean,  // if true we should do no sampling
                operation: ICancellable,
                protected reusePage: boolean) {
        super(page, operation, "histogram");
    }

    public setValue(bcs: BasicColStats): void {
        this.stats = bcs;
    }

    public setRemoteObject(ro: TableTargetAPI) {
        this.remoteObject = ro;
    }

    public onNext(value: PartialResult<BasicColStats>): void {
        super.onNext(value);
        this.setValue(value.data);
        HistogramViewBase.adjustStats(this.cd.kind, this.stats);
    }

    public histogram(): void {
        const size = PlottingSurface.getDefaultChartSize(this.page);
        let cdfCount = Math.floor(size.width);
        const bucketCount = HistogramViewBase.bucketCount(this.stats, this.page, this.cd.kind, false, true);
        if (cdfCount === 0)
            cdfCount = bucketCount;

        let samplingRate = 1.0;
        if (!this.exact)
            samplingRate = HistogramViewBase.samplingRate(bucketCount, this.stats.presentCount, this.page);

        const column = HistogramViewBase.getRange(this.stats, this.cd, this.allStrings, cdfCount);
        const args: HistogramArgs = {
            column,
            samplingRate,
            cdfSamplingRate: samplingRate,
            seed: Seed.instance.get(),
            cdfBucketCount: cdfCount,
            bucketCount,
        };
        const rr = this.remoteObject.createHistogramRequest(args);

        rr.chain(this.operation);
        const axisData = new AxisData(this.cd, this.stats, this.allStrings, bucketCount);
        const renderer = new HistogramRenderer(this.title, this.page,
            this.remoteObject.remoteObjectId, this.rowCount, this.schema,
            axisData, rr, samplingRate, this.reusePage);
        rr.invoke(renderer);
    }

    public onCompleted(): void {
        super.onCompleted();
        if (this.stats == null)
            // probably some error occurred
            return;
        if (this.stats.presentCount === 0) {
            this.page.reportError("No data in range");
            return;
        }
        this.histogram();
    }
}

/**
 * Renders a 1D histogram.
 */
export class HistogramRenderer extends Receiver<Pair<Histogram, Histogram>> {
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
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axisData: AxisData,
                operation: ICancellable,
                protected samplingRate: number,
                reusePage: boolean) {
        super(reusePage ? sourcePage : sourcePage.dataset.newPage(title, sourcePage),
            operation, "histogram");
        this.histogram = new HistogramView(remoteTableId, rowCount, schema, this.page);
        this.page.setDataView(this.histogram);
    }

    public onNext(value: PartialResult<Pair<Histogram, Histogram>>): void {
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
class MakeHistogram extends BaseRenderer {
    public constructor(private title: string,
                       page: FullPage,
                       operation: ICancellable,
                       private colDesc: IColumnDescription,
                       private rowCount: number,
                       private schema: SchemaClass,
                       protected samplingRate: number,
                       dataset: DatasetView) {
        super(page, operation, "Reload", dataset);
    }

    public run(): void {
        super.run();
        const cd = [this.colDesc];
        const rr = this.dataset.createGetCategoryRequest(this.page, cd);
        rr.invoke(new ChartObserver(this.remoteObject, this.page, rr, this.title,
            this.rowCount, this.schema,
            { exact: this.samplingRate >= 1, heatmap: false, relative: false, reusePage: false}, cd));
    }
}

export class HistogramDialog extends Dialog {
    constructor(allColumns: string[]) {
        super("1D histogram", "Display a 1D histogram of the data in a column");
        this.addSelectField("columnName", "Column", allColumns, allColumns[0], "Column to histogram");
    }

    public getColumn(): string {
        return this.getFieldValue("columnName");
    }
}

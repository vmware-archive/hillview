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
import {
    CombineOperators,
    DataRange,
    FilterDescription, HistogramArgs,
    HistogramBase,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {OnCompleteReceiver, Receiver} from "../rpc";
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
    formatNumber,
    ICancellable,
    PartialResult,
    percent,
    reorder,
    saveAs,
    significantDigits,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {ChartObserver, ChartOptions, HistogramOptions} from "./tsViewBase";
import {HeatMapRenderer} from "./heatmapView";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase {
    protected currentData: {
        cdf: HistogramBase,
        histogram: HistogramBase,
        axisData: AxisData,
        samplingRate: number,
        title: string,
        complete: boolean // True for string histograms if there are no gaps between strings
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
            bucketCount: this.currentData.histogram.buckets.length,
            columnDescription: this.currentData.axisData.description,
        };
        return result;
    }

    public static reconstruct(ser: HistogramSerialization, page: FullPage): IDataView {
        const exact: boolean = ser.exact;
        const cd: IColumnDescription = ser.columnDescription;
        const bucketCount: number = ser.bucketCount;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd == null || exact == null || schema == null || bucketCount == null)
            return null;

        const hv = new HistogramView(ser.remoteObjectId, ser.rowCount, schema, page);
        hv.histogram1D(cd.name, bucketCount, { reusePage: true, exact: exact });
        return hv;
    }

    protected static coarsen(cdf: HistogramBase, bucketCount: number): HistogramBase {
        const cdfBucketCount = cdf.buckets.length;
        if (bucketCount === cdfBucketCount)
            return cdf;

        /*
        TODO: switch to this implementation eventually.
        This does not suffer from combing.

        const buckets = [];
        const groupSize = Math.ceil(cdfBucketCount / bucketCount);
        const fullBuckets = Math.floor(cdfBucketCount / groupSize);
        for (let i = 0; i < fullBuckets; i++) {
            let sum = 0;
            for (let j = 0; j < groupSize; j++) {
                sum += value.data.buckets[i * groupSize + j];
            }
            buckets.push(sum);
        }
        if (fullBuckets < bucketCount) {
            let sum = 0;
            for (let j = fullBuckets * groupSize; j < cdfBucketCount; j++)
                sum += value.data.buckets[j];
            buckets.push(sum);
        }
        */
        const buckets = [];
        const bucketWidth = cdfBucketCount / bucketCount;
        for (let i = 0; i < bucketCount; i++) {
            let sum = 0;
            const leftBoundary = i * bucketWidth - .5;
            const rightBoundary = leftBoundary + bucketWidth;
            for (let j = Math.ceil(leftBoundary); j < rightBoundary; j++) {
                console.assert(j < cdf.buckets.length);
                sum += cdf.buckets[j];
            }
            buckets.push(sum);
        }

        return {
            buckets: buckets,
            missingData: cdf.missingData,
            outOfRange: cdf.outOfRange
        };
    }

    protected defaultBucketCount(cdfBucketCount: number): number {
        const size = PlottingSurface.getDefaultChartSize(this.page);
        const width = Math.floor(size.width);
        return Math.min(
            Resolution.maxBucketCount,
            Math.floor(width / Resolution.minBarWidth),
            cdfBucketCount);
    }

    public updateView(title: string, cdf: HistogramBase, axisData: AxisData, samplingRate: number,
                      bucketCount: number, // If 0 the bucket count will be computed
                      elapsedMs: number, complete: boolean): void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        if (cdf == null) {
            this.page.reportError("No data to display");
            return;
        }
        if (samplingRate >= 1) {
            const submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }

        if (bucketCount === 0)
            bucketCount = this.defaultBucketCount(cdf.buckets.length);
        const h = HistogramView.coarsen(cdf, bucketCount);

        this.currentData = {
            axisData: axisData,
            title: title,
            cdf: cdf,
            histogram: h,
            samplingRate: samplingRate,
            complete: complete
        };

        const counts = h.buckets;
        bucketCount = counts.length;
        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        this.plot.setHistogram(h, samplingRate, axisData);
        this.plot.draw();

        const discrete = kindIsString(this.currentData.axisData.description.kind) ||
            this.currentData.axisData.description.kind === "Integer";
        this.cdfPlot.setData(cdf, discrete);
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
        summary += formatNumber(this.rowCount) + " points";
        if (complete &&
            axisData.distinctStrings != null &&
            axisData.distinctStrings.uniqueStrings != null)
            summary += ", " + (axisData.range.max - axisData.range.min) + " distinct values";
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
        const finalRenderer = (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
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
        // TODO: Refactor this
        const rr = this.dataset.createGetCategoryRequest(this.page, cds);
        rr.invoke(new ChartObserver(this, this.page, rr, null,
            this.rowCount, this.schema,
            { exact: this.currentData.samplingRate >= 1,
                heatmap: false, relative: false, reusePage: false }, cds));
    }

    public changeBuckets(bucketCount: number): void {
        this.updateView(this.currentData.title, this.currentData.cdf,
            this.currentData.axisData, this.currentData.samplingRate,
            bucketCount, 0, this.currentData.complete);
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
        this.histogram1D(this.currentData.axisData.description.name,
            this.currentData.histogram.buckets.length,
            { exact: this.currentData.samplingRate >= 1, reusePage: true } );
    }

    public histogram1D(colName: string, bucketCount: number, options: HistogramOptions): void {
        const cd = this.schema.find(colName);
        const size = PlottingSurface.getDefaultChartSize(this.page);
        const rr = this.getDataRange(cd, size.width);
        rr.invoke(new DataRangeCollector(
            this, this.page, rr, this.schema, bucketCount, cd, null,
            size.width, options));
    }

    public exactHistogram(): void {
        if (this.currentData == null)
            return;
        this.histogram1D(this.currentData.axisData.description.name,
            this.currentData.histogram.buckets.length,
            { exact: true, reusePage: true } );
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
        // selection could be done in reverse
        [xl, xr] = reorder(xl, xr);

        const kind = this.currentData.axisData.description.kind;
        const min = HistogramViewBase.invertToNumber(xl, this.plot.xScale, kind);
        const max = HistogramViewBase.invertToNumber(xr, this.plot.xScale, kind);
        if (min > max) {
            this.page.reportError("No data selected");
            return;
        }

        const filter: FilterDescription = {
            min: min,
            max: max,
            minString: HistogramViewBase.invert(
                xl, this.plot.xScale, kind, this.currentData.axisData.distinctStrings),
            maxString: HistogramViewBase.invert(
                xr, this.plot.xScale, kind, this.currentData.axisData.distinctStrings),
            cd: this.currentData.axisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilterRequest(filter);
        const renderer = new FilterReceiver(
            filter, this.currentData.axisData.description, this.rowCount, this.schema,
            this.currentData.axisData.distinctStrings, this.currentData.samplingRate >= 1.0,
            this.page.title, this.page, rr, this.dataset);
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
        protected sourceTitle: string,
        page: FullPage,
        operation: ICancellable<RemoteObjectId>,
        dataset: DatasetView) {
        super(page, operation, "Filter", dataset);
    }

    private filterDescription(): string {
        return "Filtered " + this.filter.cd.name;
    }

    public run(): void {
        super.run();
        const title = this.filterDescription();
        const size = PlottingSurface.getDefaultChartSize(this.page);
        const rr = this.remoteObject.getDataRange(
            this.columnDescription, size.width);
        rr.invoke(new DataRangeCollector(
            this.remoteObject, this.page, rr, this.schema, 0,
            this.columnDescription, title, size.width, {
            exact: this.exact,
            reusePage: false
        }));
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

class HistogramRenderer extends Receiver<HistogramBase>  {
    private readonly histogram: HistogramView;

    constructor(protected title: string,
                sourcePage: FullPage,
                remoteTableId: string,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected bucketCount: number,
                protected axisData: AxisData,
                operation: ICancellable<HistogramBase>,
                protected samplingRate: number,
                protected complete: boolean,
                reusePage: boolean) {
        super(reusePage ? sourcePage : sourcePage.dataset.newPage(title, sourcePage),
            operation, "histogram");
        this.histogram = new HistogramView(remoteTableId, rowCount, schema, this.page);
        this.page.setDataView(this.histogram);
    }

    public onNext(value: PartialResult<HistogramBase>): void {
        super.onNext(value);
        const timeInMs = this.elapsedMilliseconds();
        this.histogram.updateView(this.title, value.data,
            this.axisData, this.samplingRate, this.bucketCount, timeInMs, this.complete);
    }
}

/**
 * Waits for column stats to be received and then initiates a histogram rendering.
 */
export class DataRangeCollector extends OnCompleteReceiver<DataRange> {
    constructor(
        protected originator: TableTargetAPI,
        page: FullPage,
        operation: ICancellable<DataRange>,
        protected schema: SchemaClass,
        protected bucketCount: number,
        protected cd: IColumnDescription,  // title of the resulting display
        protected title: string | null,
        protected width: number,
        protected options: HistogramOptions) {
        super(page, operation, "histogram");
    }

    public numericHistogram(range: DataRange): void {
        const args = HistogramViewBase.computeHistogramArgs(
            this.cd, range, 0, this.options.exact, this.page);
        const rr = this.originator.createHistogramRequest(args);
        rr.chain(this.operation);
        const axisData = HistogramViewBase.computeAxis(this.cd, range, this.bucketCount);
        const renderer = new HistogramRenderer(this.title, this.page,
            this.originator.remoteObjectId, range.presentCount, this.schema, this.bucketCount,
            axisData, rr, args.samplingRate, false, this.options.reusePage);
        rr.invoke(renderer);
    }

    private stringHistogram(range: DataRange): void {
        const cdfBucketCount = range.boundaries.length;
        const args = HistogramViewBase.computeHistogramArgs(
            this.cd, range, cdfBucketCount, this.options.exact, this.page);
        const rr = this.originator.createHistogramRequest(args);
        const axisData = HistogramViewBase.computeAxis(this.cd, range, this.bucketCount);
        const renderer = new HistogramRenderer("Histogram of " + this.cd.name,
            this.page, this.originator.remoteObjectId,
            range.presentCount, this.schema, this.bucketCount,
            axisData, rr, args.samplingRate,
            this.width > cdfBucketCount, this.options.reusePage);
        rr.invoke(renderer);
    }

    public run(value: DataRange): void {
        if (value.presentCount === 0) {
            this.page.reportError("All values are missing");
            return;
        }

        if (value.boundaries == null)
            this.numericHistogram(value);
        else
            this.stringHistogram(value);
    }
}

/**
 * This class is invoked by the ZipReceiver after a set operation to create a new histogram
 */
class MakeHistogram extends BaseRenderer {
    public constructor(private title: string,
                       page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       private colDesc: IColumnDescription,
                       private rowCount: number,
                       private schema: SchemaClass,
                       protected samplingRate: number,
                       dataset: DatasetView) {
        super(page, operation, "Reload", dataset);
    }

    public run(): void {
        super.run();
        const hv = new HistogramView(
            this.remoteObject.remoteObjectId, this.rowCount, this.schema, this.page);
        hv.histogram1D(this.colDesc.name, 0, { reusePage: true, exact: this.samplingRate >= 1.0 });
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
        protected rowCount: number,
        protected cds: IColumnDescription[],
        protected title: string | null,
        protected options: ChartOptions) {
        super(page, operation, "histogram");
    }

    public run(value: DataRange[]): void {
        console.assert(value.length === 2);
        if (value[0].presentCount === 0 || value[1].presentCount === 0) {
            this.page.reportError("No non-missing data");
            return;
        }
        const args: HistogramArgs[] = [];
        const xBucketCount = HistogramViewBase.bucketCount(this.value[0], this.page,
            this.cds[0].kind, this.options.heatmap, true);
        const yBucketCount = HistogramViewBase.bucketCount(this.value[1], this.page,
            this.cds[1].kind, this.options.heatmap, false);

        let arg = HistogramViewBase.computeHistogramArgs(
            this.cds[0], value[0], xBucketCount, this.options.exact, this.page);
        args.push(arg);
        arg = HistogramViewBase.computeHistogramArgs(
            this.cds[1], value[1], yBucketCount, this.options.exact, this.page);
        args.push(arg);

        const rr = this.originator.createHeatMapRequest(args);
        const renderer = new HeatMapRenderer(this.page,
            this.originator, this.rowCount, this.schema,
            this.cds, value,
            1.0, rr, this.options.reusePage);
        rr.chain(this.operation);
        rr.invoke(renderer);
    }
}

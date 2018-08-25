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
import {DatasetView, HistogramSerialization, IViewSerialization} from "../datasetView";
import {
    CombineOperators,
    FilterDescription,
    HistogramBase,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, ZipReceiver} from "../tableTarget";
import {CDFPlot} from "../ui/CDFPlot";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {HistogramOptions, Resolution} from "../ui/ui";
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
import {DataRangeCollector, DataRangesCollector} from "./dataRangesCollectors";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase {
    protected cdf: HistogramBase;
    protected histogram: HistogramBase;
    protected axisData: AxisData;
    protected plot: HistogramPlot;

    constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Histogram");
        this.surface = new HtmlPlottingSurface(this.chartDiv, page);
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
                { text: "group by...",
                    action: () => {
                        this.trellis();
                    },
                    help: "Group data by a third column.",
                },
            ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        if (this.samplingRate >= 1) {
            const submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
    }

    public serialize(): IViewSerialization {
        const result: HistogramSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            bucketCount: this.histogram.buckets.length,
            columnDescription: this.axisData.description,
        };
        return result;
    }

    public static reconstruct(ser: HistogramSerialization, page: FullPage): IDataView {
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (ser.columnDescription == null || ser.samplingRate == null ||
            schema == null || ser.bucketCount == null)
            return null;

        const hv = new HistogramView(
            ser.remoteObjectId, ser.rowCount, schema, ser.samplingRate, page);
        hv.setAxes(new AxisData(ser.columnDescription, null));
        return hv;
    }

    public static coarsen(cdf: HistogramBase, bucketCount: number): HistogramBase {
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
        };
    }

    public setAxes(axisData: AxisData): void {
        this.axisData = axisData;
    }

    public updateView(cdf: HistogramBase, bucketCount: number, // If 0 the bucket count will be computed
                      elapsedMs: number): void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        if (cdf == null) {
            this.page.reportError("No data to display");
            return;
        }
        if (bucketCount === 0) {
            // Compute the number of buckets to display
            const size = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
            const width = Math.floor(size.width);
            bucketCount = Math.min(
                Resolution.maxBucketCount,
                Math.floor(width / Resolution.minBarWidth),
                cdf.buckets.length);
            if (this.axisData.description.kind === "Integer")
                bucketCount = Math.min(
                    bucketCount,
                    this.axisData.range.max - this.axisData.range.min);
        }
        const h = HistogramView.coarsen(cdf, bucketCount);
        this.cdf = cdf;
        this.histogram = h;

        const counts = h.buckets;
        bucketCount = counts.length;
        this.plot.setHistogram(h, this.samplingRate, this.axisData);
        this.plot.draw();

        const discrete = kindIsString(this.axisData.description.kind) ||
            this.axisData.description.kind === "Integer";
        this.cdfPlot.setData(cdf, discrete);
        this.cdfPlot.draw();
        this.setupMouse();

        this.cdfDot =  this.surface.getChart()
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");

        const pointDesc = ["x", "y", "count"];
        if (cdf != null)
            pointDesc.push("cdf");
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(), pointDesc, 40);

        let summary = "";
        if (h.missingData !== 0)
            summary = formatNumber(h.missingData) + " missing, ";
        summary += formatNumber(this.rowCount) + " points";
        if (this.axisData != null &&
            this.axisData.range.leftBoundaries != null &&
            this.axisData.range.allStringsKnown)
            summary += ", " + this.axisData.boundaries.length + " distinct values";
        summary += ", " + String(bucketCount) + " buckets";
        if (this.samplingRate < 1.0)
            summary += ", sampling rate " + significantDigits(this.samplingRate);
        this.summary.innerHTML = summary;
    }

    public trellis(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name !== this.axisData.description.name)
                columns.push(this.schema.displayName(col.name));
        }
        if (columns.length === 0) {
            this.page.reportError("No acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column", "Select a column to group on.");
        dialog.addSelectField("column", "column", columns, null,
            "The column that will be used to group on.");
        dialog.setAction(() => this.showTrellis(dialog.getFieldValue("column")));
        dialog.show();
    }

    private showTrellis(colName: string): void {
        const groupBy = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.axisData.description, groupBy];
        const buckets = HistogramViewBase.maxHistogram2DBuckets(this.page);
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: "TrellisHistogram", exact: false
        }));
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
                title, page, operation, this.axisData.description,
                this.rowCount, this.schema, this.samplingRate >= 1,
                this.dataset);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.dataset, finalRenderer));
    }

    public chooseSecondColumn(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name === this.axisData.description.name)
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
        let line = this.axisData.description.name + ",count";
        lines.push(line);
        for (let x = 0; x < this.histogram.buckets.length; x++) {
            const bx = this.axisData.bucketDescription(x);
            const l = "" + JSON.stringify(bx) + "," + this.histogram.buckets[x];
            lines.push(l);
        }
        line = "missing," + this.histogram.missingData;
        lines.push(line);
        return lines;
    }

    private showSecondColumn(colName: string): void {
        const oc = this.schema.find(colName);
        const cds: IColumnDescription[] = [this.axisData.description, oc];
        const buckets = HistogramViewBase.maxHistogram2DBuckets(this.page);
        const rr = this.createDataRangesRequest(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.histogram.buckets.length, 0], cds, null, {
            reusePage: false,
            relative: false,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    public changeBuckets(bucketCount: number): void {
        this.updateView(this.cdf, bucketCount, 0);
    }

    public chooseBuckets(): void {
        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public resize(): void {
        if (this.cdf == null)
            return;
        this.updateView(this.cdf, this.histogram.buckets.length, 0);
    }

    public refresh(): void {
        this.histogram1D(
            this.page.title,
            this.axisData.description,
            this.histogram.buckets.length,
            { exact: this.samplingRate >= 1, reusePage: true } );
    }

    public histogram1D(title: string, cd: IColumnDescription,
                       bucketCount: number, options: HistogramOptions): void {
        const size = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const rr = this.createDataRangeRequest(cd, size.width);
        rr.invoke(new DataRangeCollector(
            this, this.page, rr, this.schema, bucketCount, cd, title,
            size.width, options));
    }

    public exactHistogram(): void {
        if (this == null)
            return;
        this.histogram1D(
            this.page.title,
            this.axisData.description,
            this.histogram.buckets.length,
            { exact: true, reusePage: true } );
    }

    protected onMouseMove(): void {
        const position = d3mouse(this.surface.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];

        let xs = "";
        if (this.axisData.scale != null)
            xs = this.axisData.invert(position[0]);
        const y = Math.round(this.plot.getYScale().invert(position[1]));
        const ys = significantDigits(y);
        const size = this.plot.get(mouseX);
        const pointDesc = [xs, ys, significantDigits(size)];

        if (this.cdfPlot != null) {
            const cdfPos = this.cdfPlot.getY(mouseX);
            this.cdfDot.attr("cx", mouseX);
            this.cdfDot.attr("cy", (1 - cdfPos) * this.surface.getChartHeight());
            const perc = percent(cdfPos);
            pointDesc.push(perc);
        }
        this.pointDescription.update(pointDesc, mouseX, mouseY);
    }

    public dragEnd(): boolean {
        if (!super.dragEnd())
            return false;
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
            columnDescription: this.axisData.description,
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
        if (this.plot == null || this.axisData.scale == null)
            return;

        // coordinates within chart
        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        // selection could be done in reverse
        [xl, xr] = reorder(xl, xr);

        const filter: FilterDescription = {
            min: this.axisData.invertToNumber(xl),
            max: this.axisData.invertToNumber(xr),
            minString: this.axisData.invert(xl),
            maxString: this.axisData.invert(xr),
            cd: this.axisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilterRequest(filter);
        const renderer = new FilterReceiver(filter, this.axisData.description, this.schema,
            this.samplingRate >= 1.0, this.page.title, this.page, rr, this.dataset);
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
        protected schema: SchemaClass,
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
        const size = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const rr = this.remoteObject.createDataRangeRequest(
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

export class HistogramRenderer extends Receiver<HistogramBase>  {
    private readonly view: HistogramView;

    constructor(protected title: string,
                sourcePage: FullPage,
                remoteTableId: string,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected bucketCount: number,
                protected axisData: AxisData,
                operation: ICancellable<HistogramBase>,
                protected samplingRate: number,
                reusePage: boolean) {
        super(reusePage ? sourcePage : sourcePage.dataset.newPage(title, sourcePage),
            operation, "histogram");
        this.view = new HistogramView(
            remoteTableId, rowCount, schema, this.samplingRate, this.page);
        this.view.setAxes(axisData);
        this.page.setDataView(this.view);
    }

    public onNext(value: PartialResult<HistogramBase>): void {
        super.onNext(value);
        const timeInMs = this.elapsedMilliseconds();
        this.view.updateView(value.data, this.bucketCount, timeInMs);
    }
}

/**
 * This class is invoked by the ZipReceiver after a set operation to create a new histogram
 */
class MakeHistogram extends BaseRenderer {
    public constructor(private title: string,
                       page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       private cd: IColumnDescription,
                       private rowCount: number,
                       private schema: SchemaClass,
                       protected exact: boolean,
                       dataset: DatasetView) {
        super(page, operation, "Reload", dataset);
    }

    public run(): void {
        super.run();
        const size = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const rr = this.remoteObject.createDataRangeRequest(this.cd, size.width);
        rr.invoke(new DataRangeCollector(
            this.remoteObject, this.page, rr, this.schema, 0, this.cd, null,
            size.width, { reusePage: false, exact: this.exact}));
    }
}

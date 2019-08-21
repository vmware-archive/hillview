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
import {HistogramSerialization, IViewSerialization} from "../datasetView";
import {
    FilterDescription,
    HistogramBase,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {CDFPlot} from "../ui/CDFPlot";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {HistogramOptions, HtmlString, Resolution} from "../ui/ui";
import {
    formatNumber,
    ICancellable,
    PartialResult,
    percent,
    reorder,
    saveAs,
    significantDigits, significantDigitsHtml,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {FilterReceiver, DataRangesCollector} from "./dataRangesCollectors";
import {IScrollTarget} from "../ui/scroll";
import {BaseRenderer} from "../tableTarget";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase implements IScrollTarget {
    protected cdf: HistogramBase;
    protected histogram: HistogramBase;
    protected plot: HistogramPlot;
    protected bucketCount: number;

    constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Histogram");

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
                    help: "Change the number of buckets used to draw this histogram. ",
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

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
            case "GAxis":
                return null;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                const range = {
                    min: 0,
                    max: this.plot.maxYAxis != null ? this.plot.maxYAxis : Math.max(...this.histogram.buckets),
                    presentCount: this.rowCount - this.histogram.missingData,
                    missingCount: this.histogram.missingData
                };
                return new AxisData(null, range);
        }
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.histogram == null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        if (eventKind === "XAxis") {
            const collector = new DataRangesCollector(this,
                this.page, null, this.schema, [0],  // any number of buckets
                [this.xAxisData.description], this.page.title, {
                    chartKind: "Histogram", exact: this.samplingRate >= 1,
                    relative: false, reusePage: true
                });
            collector.run([sourceRange]);
            collector.finished();
        } else if (eventKind === "YAxis") {
            this.updateView(this.cdf, this.bucketCount, sourceRange.max);
        }
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.surface = new HtmlPlottingSurface(this.chartDiv, this.page, {});
        this.plot = new HistogramPlot(this.surface);
        this.cdfPlot = new CDFPlot(this.surface);
    }

    public serialize(): IViewSerialization {
        const result: HistogramSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            bucketCount: this.bucketCount,
            columnDescription: this.xAxisData.description,
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
        hv.bucketCount = ser.bucketCount;
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

    public setAxes(xAxisData: AxisData): void {
        this.xAxisData = xAxisData;
    }

    /**
     * @param cdf: Data for the cdf.
     * @param bucketCount: Number of buckets to display.  If 0 the bucket count will be computed
     * @param maxYAxis: maximum value to use for Y axis if not null
     */
    public updateView(cdf: HistogramBase, bucketCount: number, maxYAxis: number | null): void {
        this.createNewSurfaces();
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
            if (this.xAxisData.description.kind === "Integer")
                bucketCount = Math.min(
                    bucketCount,
                    this.xAxisData.range.max - this.xAxisData.range.min);
        }
        this.bucketCount = bucketCount;
        const h = HistogramView.coarsen(cdf, bucketCount);
        this.cdf = cdf;
        this.histogram = h;

        const counts = h.buckets;
        bucketCount = counts.length;
        this.plot.setHistogram(h, this.samplingRate, this.xAxisData, maxYAxis);
        this.plot.draw();

        const discrete = kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer";
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

        let summary = new HtmlString("");
        if (h.missingData !== 0)
            summary = summary.appendSafeString(formatNumber(h.missingData) + " missing, ");
        summary = summary.appendSafeString(formatNumber(this.rowCount) + " points");
        if (this.xAxisData != null &&
            this.xAxisData.range.leftBoundaries != null &&
            this.xAxisData.range.allStringsKnown)
            summary = summary.appendSafeString(
                ", " + this.xAxisData.range.leftBoundaries.length + " distinct values");
        summary = summary.appendSafeString(
            ", " + String(bucketCount) + " buckets");
        if (this.samplingRate < 1.0)
            summary = summary.appendSafeString(", sampling rate ")
                .append(significantDigitsHtml(this.samplingRate));
        summary.setInnerHtml(this.summary);
    }

    public trellis(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name !== this.xAxisData.description.name)
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
        const cds: IColumnDescription[] = [this.xAxisData.description, groupBy];
        const rr = this.createDataRangesRequest(cds, this.page, "TrellisHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: "TrellisHistogram", exact: false
        }));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseRenderer {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(
                title, [this.axisData.description], this.schema, [0],
                page, operation, this.dataset, null, {
                    exact: this.samplingRate >= 1, reusePage: false,
                    relative: false, chartKind: "Histogram" });
        };
    }

    public chooseSecondColumn(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name === this.xAxisData.description.name)
                continue;
            columns.push(this.schema.displayName(col.name));
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
    }

    /**
     * Convert the data to text.
     * @returns An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [];
        let line = this.schema.displayName(this.xAxisData.description.name) + ",count";
        lines.push(line);
        for (let x = 0; x < this.histogram.buckets.length; x++) {
            const bx = this.xAxisData.bucketDescription(x, 0);
            const l = "" + JSON.stringify(bx) + "," + this.histogram.buckets[x];
            lines.push(l);
        }
        line = "missing," + this.histogram.missingData;
        lines.push(line);
        return lines;
    }

    private showSecondColumn(colName: string): void {
        const oc = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.xAxisData.description, oc];
        const rr = this.createDataRangesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.histogram.buckets.length, 0], cds, null, {
            reusePage: false,
            relative: false,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    public changeBuckets(bucketCount: number): void {
        this.updateView(this.cdf, bucketCount, this.plot.maxYAxis);
    }

    public chooseBuckets(): void {
        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public resize(): void {
        if (this.cdf == null)
            return;
        this.updateView(this.cdf, this.bucketCount, this.plot.maxYAxis);
    }

    public refresh(): void {
        this.histogram1D(
            this.page.title,
            this.xAxisData.description,
            this.bucketCount,
            { exact: this.samplingRate >= 1, reusePage: true } );
    }

    public histogram1D(title: PageTitle, cd: IColumnDescription,
                       bucketCount: number, options: HistogramOptions): void {
        const rr = this.createDataRangesRequest([cd], this.page, "Histogram");
        rr.invoke(new DataRangesCollector(
            this, this.page, rr, this.schema, [bucketCount], [cd], title,
            { chartKind: "Histogram", relative: false, reusePage: options.reusePage, exact: options.exact }));
    }

    public exactHistogram(): void {
        if (this == null)
            return;
        this.histogram1D(
            this.page.title,
            this.xAxisData.description,
            this.bucketCount,
            { exact: true, reusePage: true } );
    }

    protected onMouseMove(): void {
        const position = d3mouse(this.surface.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];

        let xs = "";
        if (this.xAxisData.scale != null)
            xs = this.xAxisData.invert(position[0]);
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
        const newPage = this.dataset.newPage(
            new PageTitle("Table"), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage, null);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
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
        if (this.plot == null || this.xAxisData.scale == null)
            return;

        // coordinates within chart
        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        // selection could be done in reverse
        [xl, xr] = reorder(xl, xr);

        const filter: FilterDescription = {
            min: this.xAxisData.invertToNumber(xl),
            max: this.xAxisData.invertToNumber(xr),
            minString: this.xAxisData.invert(xl),
            maxString: this.xAxisData.invert(xr),
            cd: this.xAxisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilterRequest(filter);
        const title = new PageTitle("Filtered " + this.schema.displayName(this.axisData.description.name));
        const renderer = new FilterReceiver(title, [this.axisData.description], this.schema,
					    [0], this.page, rr, this.dataset, filter, {
		exact: this.samplingRate >= 1, reusePage: false, relative: false, chartKind: "Histogram",
            });
        rr.invoke(renderer);
    }

    public pageDown(): void {
        // TODO
    }

    public pageUp(): void {
        // TODO
    }

    public scrolledTo(position: number): void {
        // TODO
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

    constructor(protected title: PageTitle,
                sourcePage: FullPage,
                remoteTableId: string,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected bucketCount: number,
                protected xAxisData: AxisData,
                operation: ICancellable<HistogramBase>,
                protected samplingRate: number,
                reusePage: boolean) {
        super(reusePage ? sourcePage : sourcePage.dataset.newPage(title, sourcePage),
            operation, "histogram");
        this.view = new HistogramView(
            remoteTableId, rowCount, schema, this.samplingRate, this.page);
        this.view.setAxes(xAxisData);
        this.page.setDataView(this.view);
    }

    public onNext(value: PartialResult<HistogramBase>): void {
        super.onNext(value);
        this.view.updateView(value.data, this.bucketCount, null);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

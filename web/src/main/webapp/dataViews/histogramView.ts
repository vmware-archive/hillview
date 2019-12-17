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
    AugmentedHistogram,
    FilterDescription,
    Histogram,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {DisplayName, SchemaClass} from "../schemaClass";
import {CDFPlot} from "../ui/CDFPlot";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {PiePlot} from "../ui/piePlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {HtmlString, Resolution, SpecialChars} from "../ui/ui";
import {
    formatNumber,
    ICancellable, makeInterval,
    Pair,
    PartialResult,
    percent,
    reorder,
    saveAs,
    significantDigits, significantDigitsHtml,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {FilterReceiver, DataRangesReceiver} from "./dataRangesCollectors";
import {BaseReceiver} from "../tableTarget";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase /*implements IScrollTarget*/ {
    protected cdf: AugmentedHistogram;
    protected augmentedHistogram: AugmentedHistogram;
    protected histogram: Histogram;
    protected plot: HistogramPlot | PiePlot;
    protected bucketCount: number;
    protected pie: boolean;

    constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected samplingRate: number,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Histogram");

        this.pie = false;
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
                { text: "pie chart/histogram",
                    action: () => this.togglePie(),
                    help: "Draw the data as a pie chart or as a histogram.",
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
                return new AxisData(null, range, 0);
        }
    }

    private togglePie(): void {
        this.pie = !this.pie;
        this.resize();
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.histogram == null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        if (eventKind === "XAxis") {
            const collector = new DataRangesReceiver(this,
                this.page, null, this.schema, [0],  // any number of buckets
                [this.xAxisData.description], this.page.title, {
                    chartKind: "Histogram", exact: this.samplingRate >= 1,
                    relative: false, reusePage: true
                });
            collector.run([sourceRange]);
            collector.finished();
        } else if (eventKind === "YAxis") {
            // TODO
            this.updateView(this.cdf, this.augmentedHistogram, sourceRange.max);
        }
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.surface = new HtmlPlottingSurface(this.chartDiv, this.page, {});
        if (this.pie) {
            this.plot = new PiePlot(this.surface);
            this.cdfPlot = null;
        } else {
            this.plot = new HistogramPlot(this.surface);
            this.cdfPlot = new CDFPlot(this.surface);
        }
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
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
        hv.setAxes(new AxisData(ser.columnDescription, null, ser.bucketCount));
        hv.bucketCount = ser.bucketCount;
        return hv;
    }

    public setAxes(xAxisData: AxisData): void {
        this.xAxisData = xAxisData;
    }

    /**
     * @param cdf: Data for the cdf.
     * @param augmentedHistogram: Data for the histogram buckets.
     * @param maxYAxis: maximum value to use for Y axis if not null
     */
    public updateView(cdf: AugmentedHistogram, augmentedHistogram: AugmentedHistogram,
                      maxYAxis: number | null): void {
        this.createNewSurfaces();
        if (augmentedHistogram == null) {
            this.page.reportError("No data to display");
            return;
        }
        this.augmentedHistogram = augmentedHistogram;
        this.rowCount = augmentedHistogram.histogram.buckets.reduce((a, b) => a + b, 0);
        if (this.isPrivate()) {
            const cols = [this.xAxisData.description.name];
            const eps = this.dataset.getEpsilon(cols);
            this.page.setEpsilon(eps, cols);
        }

        const h = augmentedHistogram.histogram;
        this.histogram = h;
        this.cdf = cdf;

        const counts = h.buckets;
        this.bucketCount = counts.length;
        this.plot.setHistogram(augmentedHistogram, this.samplingRate, h.missingData,
                               this.xAxisData, maxYAxis, this.page.dataset.isPrivate());
        this.plot.draw();

        const discrete = kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer";
        if (this.cdfPlot != null) {
            this.cdfPlot.setData(cdf.cdfBuckets, discrete);
            this.cdfPlot.draw();
        }
        this.setupMouse();

        if (!this.pie)
            this.cdfDot =  this.surface.getChart()
                .append("circle")
                .attr("r", Resolution.mouseDotRadius)
                .attr("fill", "blue");

        const pointDesc = ["x", "y", "count"];
        pointDesc.push("cdf");
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(), pointDesc, 40);

        let summary = new HtmlString("");
        if (h.missingData !== 0) {
            if (this.isPrivate())
                summary.appendSafeString(SpecialChars.approx);
            summary = summary.appendSafeString(formatNumber(h.missingData) + " missing, ");
        }
        if (this.isPrivate())
            summary.appendSafeString(SpecialChars.approx);
        summary = summary.appendSafeString(formatNumber(this.rowCount) + " points");
        if (this.xAxisData != null &&
            this.xAxisData.range.stringQuantiles != null &&
            this.xAxisData.range.allStringsKnown)
            summary = summary.appendSafeString(
                ", " + this.xAxisData.range.stringQuantiles.length + " distinct values");
        summary = summary.appendSafeString(
            ", " + String(this.bucketCount) + " buckets");
        if (this.samplingRate < 1.0)
            summary = summary.appendSafeString(", sampling rate ")
                .append(significantDigitsHtml(this.samplingRate));
        summary.setInnerHtml(this.summary);
    }

    public trellis(): void {
        const columns: DisplayName[] = this.schema.displayNamesExcluding([this.xAxisData.description.name]);
        this.chooseTrellis(columns);
    }

    protected showTrellis(colName: DisplayName): void {
        const groupBy = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.xAxisData.description, groupBy];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: "TrellisHistogram", exact: false
        }));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(title, [this.xAxisData.description], this.schema,
                [0], page, operation, this.dataset, {
                exact: this.samplingRate >= 1, reusePage: false,
                relative: false, chartKind: "Histogram"
            });
        };
    }

    public chooseSecondColumn(): void {
        const columns: DisplayName[] = [];
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
        dialog.addColumnSelectField("column", "column", columns, null,
            "The second column that will be used in addition to the one displayed here " +
            "for drawing a two-dimensional histogram.");
        dialog.setAction(() => this.showSecondColumn(dialog.getColumnName("column")));
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

    private showSecondColumn(colName: DisplayName): void {
        const oc = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.xAxisData.description, oc];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.histogram.buckets.length, 0], cds, null, {
            reusePage: false,
            relative: false,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    public changeBuckets(bucketCount: number): void {
        const rr = this.createDataQuantilesRequest([this.xAxisData.description],
            this.page, "Histogram");
        const exact = this.isPrivate() || this.samplingRate >= 1;
        rr.invoke(new DataRangesReceiver(
            this, this.page, rr, this.schema, [bucketCount], [this.xAxisData.description], null,
            { chartKind: "Histogram", relative: false, exact: exact, reusePage: true }));
    }

    public chooseBuckets(): void {
        const bucketDialog = new BucketDialog();
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public resize(): void {
        if (this.cdf == null)
            return;
        this.updateView(this.cdf, this.augmentedHistogram, this.plot.maxYAxis);
    }

    public refresh(): void {
        const ranges = [this.xAxisData.dataRange];
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema, [this.xAxisData.bucketCount],
            [this.xAxisData.description], this.page.title, {
                chartKind: "Histogram", exact: this.samplingRate >= 1,
                relative: false, reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public exactHistogram(): void {
        if (this == null)
            return;
        this.samplingRate = 1.0;
        this.refresh();
    }

    protected onMouseMove(): void {
        if (this.pie) {
            // TODO
        } else {
            const position = d3mouse(this.surface.getChart().node());
            const mouseX = position[0];
            const mouseY = position[1];

            let xs = "";
            if (this.xAxisData.scale != null)
                xs = this.xAxisData.invert(position[0]);
            const y = Math.round(this.plot.getYScale().invert(position[1]));
            const ys = significantDigits(y);
            const size = this.plot.get(mouseX);
            const pointDesc = [xs, ys, makeInterval(size)];

            if (this.cdfPlot != null) {
                const cdfPos = this.cdfPlot.getY(mouseX);
                this.cdfDot.attr("cx", mouseX);
                this.cdfDot.attr("cy", (1 - cdfPos) * this.surface.getChartHeight());
                const perc = percent(cdfPos);
                pointDesc.push(perc);
            }
            this.pointDescription.update(pointDesc, mouseX, mouseY);
        }
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
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage);
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
        const title = new PageTitle("Filtered " + this.schema.displayName(this.xAxisData.description.name));
        const renderer = new FilterReceiver(title, [this.xAxisData.description], this.schema,
            [0], this.page, rr, this.dataset, {
            exact: this.samplingRate >= 1,
            reusePage: false,
            relative: false,
            chartKind: "Histogram"
        });
        rr.invoke(renderer);
    }
}

export class HistogramReceiver extends Receiver<Pair<AugmentedHistogram, AugmentedHistogram>>  {
    private readonly view: HistogramView;

    constructor(protected title: PageTitle,
                sourcePage: FullPage,
                remoteTableId: string,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected bucketCount: number,
                protected xAxisData: AxisData,
                operation: ICancellable<Pair<AugmentedHistogram, AugmentedHistogram>>,
                protected samplingRate: number,
                reusePage: boolean) {
        super(reusePage ? sourcePage : sourcePage.dataset.newPage(title, sourcePage),
            operation, "histogram");
        this.view = new HistogramView(
            remoteTableId, rowCount, schema, this.samplingRate, this.page);
        this.view.setAxes(xAxisData);
        this.page.setDataView(this.view);
    }

    public onNext(value: PartialResult<Pair<AugmentedHistogram, AugmentedHistogram>>): void {
        super.onNext(value);
        if (value == null || value.data == null || value.data.first == null)
            return;
        const histogram = value.data.first;
        const cdf = value.data.second;
        this.view.updateView(cdf, histogram, null);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

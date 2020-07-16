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
    Groups,
    IColumnDescription, kindIsNumeric,
    kindIsString,
    RemoteObjectId, RangeFilterArrayDescription,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {DisplayName, SchemaClass} from "../schemaClass";
import {CDFPlot, NoCDFPlot} from "../ui/cdfPlot";
import {IDataView} from "../ui/dataview";
import {Dialog, saveAs} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {HistogramPlot} from "../ui/histogramPlot";
import {PiePlot} from "../ui/piePlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {DragEventKind, Resolution} from "../ui/ui";
import {
    histogramAsCsv,
    Converters,
    ICancellable, makeInterval,
    PartialResult,
    percentString,
    significantDigits, Two,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NewTargetReceiver, DataRangesReceiver} from "./dataRangesReceiver";
import {BaseReceiver} from "../modules";
import {CommonArgs} from "../ui/receiver";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class HistogramView extends HistogramViewBase<Two<Two<Groups<number>>>> /*implements IScrollTarget*/ {
    protected plot: HistogramPlot | PiePlot;
    protected bucketCount: number;
    readonly defaultProvenance = "from histogram";

    constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        protected samplingRate: number,
        protected pie: boolean,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Histogram");

        this.menu = new TopMenu([this.exportMenu(), {
            text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                {
                    text: "refresh",
                    action: () => {
                        this.refresh();
                    },
                    help: "Redraw this view.",
                },
                {
                    text: "table",
                    action: () => this.showTable([this.xAxisData.description], this.defaultProvenance),
                    help: "Show the data underlying this histogram using a table view.",
                },
                {
                    text: "pie chart/histogram",
                    action: () => this.togglePie(),
                    help: "Draw the data as a pie chart or as a histogram.",
                },
                {
                    text: "exact",
                    action: () => this.exactHistogram(),
                    help: "Draw this histogram without making any approximations.",
                },
                {
                    text: "# buckets...",
                    action: () => this.chooseBuckets(),
                    help: "Change the number of buckets used to draw this histogram. ",
                },
                {
                    text: "correlate...",
                    action: () => this.chooseSecondColumn(),
                    help: "Draw a 2-dimensional histogram using this data and another column.",
                },
                {
                    text: "quartiles...",
                    action: () => this.chooseQuartilesColumn(),
                    help: "Draw quartiles of a numeric column for each bucket of this histogram.",
                },
                {
                    text: "group by...",
                    action: () => {
                        this.trellis();
                    },
                    help: "Group data by a third column.",
                },
            ])
        },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        const submenu = this.menu.getSubmenu("View");
        if (this.samplingRate >= 1) {
            submenu.enable("exact", false);
        }
        if (this.isPrivate()) {
            this.menu.enable("Combine", false);
            submenu.enable("correlate...", false);
        }
    }

    public dragStart(): void {
        if (!this.pie)
            super.dragStart();
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
                    max: this.plot.maxYAxis != null ?
                        this.plot.maxYAxis : Math.max(...this.histogram().first.perBucket),
                    presentCount: this.rowCount - this.histogram().first.perMissing,
                    missingCount: this.histogram().first.perMissing
                };
                return new AxisData(null, range, 0);
        }
    }

    private togglePie(): void {
        this.pie = !this.pie;
        this.resize();
    }

    protected cdf(): Two<Groups<number>> {
        return this.data.second;
    }

    protected histogram(): Two<Groups<number>> {
        return this.data.first;
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.data == null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        if (eventKind === "XAxis") {
            const collector = new DataRangesReceiver(this,
                this.page, null, this.schema, [0],  // any number of buckets
                [this.xAxisData.description], this.page.title,
                Converters.eventToString(pageId, eventKind), {
                    chartKind: "Histogram", exact: this.samplingRate >= 1,
                    relative: false, reusePage: true, pieChart: this.pie
                });
            collector.run([sourceRange]);
            collector.finished();
        } else if (eventKind === "YAxis") {
            // TODO
            this.updateView(this.data, sourceRange.max);
        }
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.surface = new HtmlPlottingSurface(this.chartDiv, this.page, {});
        if (this.pie) {
            this.plot = new PiePlot(this.surface);
            this.cdfPlot = new NoCDFPlot();
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
            isPie: this.pie
        };
        return result;
    }

    public static reconstruct(ser: HistogramSerialization, page: FullPage): IDataView {
        const args: CommonArgs = this.validateSerialization(ser);
        if (args == null ||
            ser.columnDescription == null || ser.samplingRate == null ||
            ser.bucketCount == null)
            return null;

        const hv = new HistogramView(
            ser.remoteObjectId, ser.rowCount, args.schema, ser.samplingRate, ser.isPie, page);
        hv.setAxis(new AxisData(ser.columnDescription, null, ser.bucketCount));
        hv.bucketCount = ser.bucketCount;
        return hv;
    }

    public setAxis(xAxisData: AxisData): void {
        // Note that the axis data has all information for the CDF plot, so the number
        // of buckets/labels in the axisData is not the same as in the histogram.
        this.xAxisData = xAxisData;
        const submenu = this.menu.getSubmenu("View");
        submenu.enable("pie chart/histogram", kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer");
    }

    /**
     * @param histogramAndCdf: first is a histogram (with confidences), second is a cdf (with confidences).
     * @param maxYAxis: maximum value to use for Y axis if not null
     */
    public updateView(histogramAndCdf: Two<Two<Groups<number>>>,
                      maxYAxis: number | null): void {
        this.createNewSurfaces();
        if (histogramAndCdf == null) {
            this.page.reportError("No data to display");
            return;
        }
        this.data = histogramAndCdf;
        // The following is only an *estimate* of the actual row count
        // this.rowCount = this.histogram.buckets.reduce(add, 0);
        if (this.isPrivate()) {
            const cols = [this.xAxisData.description.name];
            const eps = this.dataset.getEpsilon(cols);
            this.page.setEpsilon(eps, cols);
        }

        const counts = this.histogram().first.perBucket;
        this.bucketCount = counts.length;
        this.plot.setHistogram(
            this.histogram(), this.samplingRate, this.xAxisData, maxYAxis,
            this.page.dataset.isPrivate(), this.rowCount);
        this.plot.draw();

        const discrete = kindIsString(this.xAxisData.description.kind) ||
            this.xAxisData.description.kind === "Integer";
        this.cdfPlot.setData(this.cdf().first.perBucket, discrete);
        this.cdfPlot.draw();
        this.setupMouse();

        if (!this.pie)
            this.cdfDot =  this.surface.getChart()
                .append("circle")
                .attr("r", Resolution.mouseDotRadius)
                .attr("fill", "blue");

        const pointDesc = ["x", "bucket", "y", "count"];
        pointDesc.push("cdf");
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(), pointDesc, 40);

        if (this.histogram().first.perMissing !== 0)
            this.summary.set("missing", this.histogram().first.perMissing, this.isPrivate());
        this.standardSummary();
        if (this.xAxisData != null &&
            this.xAxisData.displayRange.stringQuantiles != null &&
            this.xAxisData.displayRange.allStringsKnown)
            this.summary.set("distinct values", this.xAxisData.displayRange.stringQuantiles.length);
        this.summary.set("buckets", this.bucketCount);
        if (this.samplingRate < 1.0)
            this.summary.set("sampling rate", this.samplingRate);
        this.summary.display();
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
            [0, 0], cds, null, this.defaultProvenance, {
            reusePage: false, relative: false, pieChart: false,
            chartKind: "TrellisHistogram", exact: false
        }));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new NewTargetReceiver(title, [this.xAxisData.description], this.schema,
                [0], page, operation, this.dataset, {
                exact: this.samplingRate >= 1, reusePage: false, pieChart: this.pie,
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

    public chooseQuartilesColumn(): void {
        const columns: DisplayName[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name === this.xAxisData.description.name ||
                !kindIsNumeric(col.kind))
                continue;
            columns.push(this.schema.displayName(col.name));
        }
        if (columns.length === 0) {
            this.page.reportError("No other acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column", "Select a second column to display quartiles.");
        dialog.addColumnSelectField("column", "column", columns, null,
            "The second column that will be used in addition to the one displayed here " +
            "for drawing histogram with quantiles of the second column distributions.");
        dialog.setAction(() => this.showQuartiles(dialog.getColumnName("column")));
        dialog.setCacheTitle("HistogramQuartiles");
        dialog.show();
    }

    public export(): void {
        const lines: string[] = histogramAsCsv(this.histogram().first, this.schema, this.xAxisData);
        const fileName = "histogram.csv";
        saveAs(fileName, lines.join("\n"));
    }

    private showSecondColumn(colName: DisplayName): void {
        const oc = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.xAxisData.description, oc];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [this.histogram().first.perBucket.length, 0], cds, null, this.defaultProvenance,{
            reusePage: false,
            relative: false,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    private showQuartiles(colName: DisplayName): void {
        const oc = this.schema.findByDisplayName(colName);
        const qhr = new DataRangesReceiver(this, this.page, null,
            this.schema, [this.xAxisData.bucketCount],
            [this.xAxisData.description, oc],
            null, this.defaultProvenance, {
                reusePage: false, chartKind: "QuartileVector"
            });
        qhr.run([this.xAxisData.dataRange]);
        qhr.onCompleted();
    }

    public changeBuckets(bucketCount: number): void {
        if (bucketCount == null)
            return;
        const rr = this.createDataQuantilesRequest([this.xAxisData.description],
            this.page, "Histogram");
        const exact = this.isPrivate() || this.samplingRate >= 1;
        rr.invoke(new DataRangesReceiver(
            this, this.page, rr, this.schema, [bucketCount], [this.xAxisData.description], null,
            "changed buckes", { chartKind: "Histogram", relative: false, exact: exact, reusePage: true, pieChart: this.pie }));
    }

    public chooseBuckets(): void {
        const bucketDialog = new BucketDialog(this.bucketCount, Resolution.maxBuckets(this.page.getWidthInPixels()));
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public resize(): void {
        if (this.data == null)
            return;
        this.updateView(this.data, this.plot.maxYAxis);
    }

    public refresh(): void {
        const ranges = [this.xAxisData.dataRange];
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema, [this.xAxisData.bucketCount],
            [this.xAxisData.description], this.page.title, null,{
                chartKind: "Histogram", exact: this.samplingRate >= 1,
                relative: false, reusePage: true, pieChart: this.pie
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
            const hp = this.plot as HistogramPlot;

            const position = d3mouse(this.surface.getChart().node());
            const mouseX = position[0];
            const mouseY = position[1];

            let xs = "";
            let bucketDesc = "";
            if (this.xAxisData.scale != null) {
                xs = this.xAxisData.invert(position[0]);
                const bucket = hp.getBucketIndex(position[0]);
                bucketDesc = this.xAxisData.bucketDescription(bucket, 20);
            }
            const y = Math.round(hp.getYScale().invert(position[1]));
            const ys = significantDigits(y);
            const size = hp.get(mouseX);
            const pointDesc = [xs, bucketDesc, ys, makeInterval(size)];
            const cdfPos = this.cdfPlot.getY(mouseX);
            this.cdfDot.attr("cx", mouseX);
            this.cdfDot.attr("cy", (1 - cdfPos) * this.surface.getChartHeight());
            const perc = percentString(cdfPos);
            pointDesc.push(perc);
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
        const filter: RangeFilterArrayDescription = {
            filters: [this.xAxisData.getFilter(xl, xr)],
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilterRequest(filter);
        const title = new PageTitle(this.page.title.format,
            Converters.filterArrayDescription(filter));
        const renderer = new NewTargetReceiver(title, [this.xAxisData.description], this.schema,
            [0], this.page, rr, this.dataset, {
            exact: this.samplingRate >= 1,
            reusePage: false,
            relative: false, pieChart: this.pie,
            chartKind: "Histogram"
        });
        rr.invoke(renderer);
    }
}

export class HistogramReceiver extends Receiver<Two<Two<Groups<number>>>>  {
    private readonly view: HistogramView;

    constructor(protected title: PageTitle,
                sourcePage: FullPage,
                remoteTableId: string,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected xAxisData: AxisData,
                operation: ICancellable<Two<Two<Groups<number>>>>,
                protected samplingRate: number,
                protected isPie: boolean,
                reusePage: boolean) {
        super(reusePage ? sourcePage : sourcePage.dataset.newPage(title, sourcePage),
            operation, "histogram");
        this.view = new HistogramView(
            remoteTableId, rowCount, schema, this.samplingRate, this.isPie, this.page);
        this.view.setAxis(xAxisData);
        this.page.setDataView(this.view);
    }

    public onNext(value: PartialResult<Two<Two<Groups<number>>>>): void {
        super.onNext(value);
        if (value == null || value.data == null || value.data.first == null)
            return;
        this.view.updateView(value.data, null);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

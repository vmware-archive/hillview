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
import {IViewSerialization, QuantileVectorSerialization} from "../datasetView";
import {
    FilterDescription,
    IColumnDescription,
    RecordOrder,
    RemoteObjectId, QuantilesVector,
} from "../javaBridge";
import {Receiver, RpcRequest} from "../rpc";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {IDataView} from "../ui/dataview";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {ChartOptions, HtmlString, Resolution} from "../ui/ui";
import {
    ICancellable,
    PartialResult,
    reorder,
    saveAs, significantDigits,
    significantDigitsHtml,
} from "../util";
import {AxisData} from "./axisData";
import {BucketDialog, HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {FilterReceiver, DataRangesReceiver} from "./dataRangesCollectors";
import {DisplayName, SchemaClass} from "../schemaClass";
import {Quantiles2DPlot} from "../ui/quantiles2DPlot";

/**
 * This class is responsible for rendering a histogram of one colum where
 * each interval shows a whisker plot with quantiles of a second column.
 */
export class Quantiles2DView extends HistogramViewBase {
    protected yAxisData: AxisData;
    protected xPoints: number;
    protected data: QuantilesVector;
    protected plot: Quantiles2DPlot;

    constructor(remoteObjectId: RemoteObjectId, rowCount: number,
                schema: SchemaClass, protected samplingRate: number, page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "2DQuantiles");

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
                text: "heatmap",
                action: () => { this.doHeatmap(); },
                help: "Plot this data as a heatmap view.",
            }]) },
            page.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        if (this.samplingRate >= 1) {
            const submenu = this.menu.getSubmenu("View");
            submenu.enable("exact", false);
        }
    }

    protected showTrellis(colName: DisplayName): void {
        throw new Error("Method not implemented.");
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        this.surface = new HtmlPlottingSurface(this.chartDiv, this.page, {});
        this.plot = new Quantiles2DPlot(this.surface);
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
                    min: this.plot.min,
                    max: this.plot.max,
                    presentCount: this.xAxisData.range.presentCount,
                    missingCount: this.xAxisData.range.missingCount
                };
                return new AxisData(null, range, this.yAxisData.bucketCount);
        }
        return null;
    }

    public updateView(qv: QuantilesVector): void {
        this.createNewSurfaces();
        this.data = qv;
        //this.cdf = cdf;

        const bucketCount = this.xPoints;
        const canvas = this.surface.getCanvas();

        this.plot.setData(qv, 1.0, this.schema, this.xAxisData, false);
        this.plot.draw();

        //this.cdfPlot.setData(cdf.buckets, discrete);
        //this.cdfPlot.draw();

        this.setupMouse();
        this.cdfDot = canvas
            .append("circle")
            .attr("r", Resolution.mouseDotRadius)
            .attr("fill", "blue");

        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getDisplayNameString(this.schema),
                this.yAxisData.getDisplayNameString(this.schema),
                "y", "count", "%", "cdf"], 40);
        this.pointDescription.show(false);
        let summary = new HtmlString(String(bucketCount) + " buckets");
        if (this.samplingRate < 1.0)
            summary = summary.appendSafeString(", sampling rate ")
                .append(significantDigitsHtml(this.samplingRate));
        summary.setInnerHtml(this.summary);
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result: QuantileVectorSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yAxisData.description,
            xBucketCount: this.xPoints,
        };
        return result;
    }

    public static reconstruct(ser: QuantileVectorSerialization, page: FullPage): IDataView {
        const samplingRate: number = ser.samplingRate;
        const cd0: IColumnDescription = ser.columnDescription0;
        const cd1: IColumnDescription = ser.columnDescription1;
        const xPoints: number = ser.xBucketCount;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd0 === null || cd1 === null || samplingRate === null || schema === null ||
            xPoints === null)
            return null;

        const hv = new Quantiles2DView(ser.remoteObjectId, ser.rowCount, schema, samplingRate, page);
        hv.setAxis(new AxisData(cd0, null, ser.xBucketCount));
        hv.xPoints = xPoints;
        return hv;
    }

    public setAxis(xAxisData: AxisData): void {
        this.xAxisData = xAxisData;
    }

    public doHeatmap(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false,
            chartKind: "Heatmap",
            exact: true
        }));
    }

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "quantiles2d.csv";
        saveAs(fileName, lines.join("\n"));
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [];
        /*
        TODO
        let line = "";
        for (let y = 0; y < this.yAxisData.bucketCount; y++) {
            const by = this.yAxisData.bucketDescription(y, 0);
            line += "," + JSON.stringify(this.schema.displayName(this.yAxisData.description.name) + " " + by);
        }
        line += ",missing";
        lines.push(line);
        for (let x = 0; x < this.xAxisData.bucketCount; x++) {
            const data = this.qv.buckets[x];
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
         */
        return lines;
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(title, [this.xAxisData.description, this.yAxisData.description],
                this.schema, [0, 0], page, operation, this.dataset, {
                exact: this.samplingRate >= 1, chartKind: "2DQuantiles",
                reusePage: false
            });
        };
    }

    public swapAxes(): void {
        if (this == null)
            return;
        const cds = [this.yAxisData.description, this.xAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DQuantiles");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: true,
            chartKind: "2DQuantiles", exact: this.samplingRate >= 1.0
        }));
    }

    public changeBuckets(bucketCount: number): void {
        if (bucketCount == null)
            return;
        const cds = [this.xAxisData.description, this.yAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "2DQuantiles");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            [bucketCount], cds, null, {
            reusePage: true,
            chartKind: "2DQuantiles",
            exact: true
        }));
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.data == null)
            return;

        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange == null)
            return;

        if (eventKind === "XAxis") {
            const collector = new DataRangesReceiver(this,
                this.page, null, this.schema, [0, 0],  // any number of buckets
                [this.xAxisData.description, this.yAxisData.description], this.page.title, {
                    chartKind: "2DHistogram", exact: this.samplingRate >= 1,
                    reusePage: true,
                });
            collector.run([sourceRange, this.yAxisData.dataRange]);
            collector.finished();
        } else if (eventKind === "YAxis") {
            this.updateView(this.data);
        }
    }

    public chooseBuckets(): void {
        if (this == null)
            return;

        const bucketDialog = new BucketDialog(this.xPoints);
        bucketDialog.setAction(() => this.changeBuckets(bucketDialog.getBucketCount()));
        bucketDialog.show();
    }

    public resize(): void {
        if (this == null)
            return;
        this.updateView(this.data);
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description];
        const ranges = [this.xAxisData.dataRange, this.yAxisData.dataRange];
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema, [this.xAxisData.bucketCount, this.yAxisData.bucketCount],
            cds, this.page.title, {
                chartKind: "2DHistogram", exact: this.samplingRate >= 1,
                reusePage: true
            });
        collector.run(ranges);
        collector.finished();
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

        //const pos = this.cdfPlot.getY(mouseX);
        //this.cdfDot.attr("cx", mouseX + this.surface.leftMargin);
        //this.cdfDot.attr("cy", (1 - pos) * this.surface.getChartHeight() + this.surface.topMargin);
        this.pointDescription.update([xs, ys], mouseX, mouseY);
    }

    protected dragEnd(): boolean {
        if (!super.dragEnd())
            return false;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x);
        return true;
    }

    /**
     * * xl and xr are coordinates of the mouse position within the
     * canvas or legend rectangle respectively.
     */
    protected selectionCompleted(xl: number, xr: number): void {
        let selectedAxis: AxisData;
        [xl, xr] = reorder(xl, xr);

        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        selectedAxis = this.xAxisData;

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
            [this.xAxisData.description, this.yAxisData.description], this.schema,
            [this.xPoints], this.page, rr, this.dataset, {
            exact: this.samplingRate >= 1.0,
            chartKind: "2DQuantiles",
            reusePage: false,
        });
        rr.invoke(renderer);
    }

    // show the table corresponding to the data in the histogram
    protected showTable(): void {
        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true,
        }, {
            columnDescription: this.yAxisData.description,
            isAscending: true,
        } ]);

        const page = this.dataset.newPage(new PageTitle("Table"), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        page.setDataView(table);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }
}

export class Quantiles2DReceiver extends Receiver<QuantilesVector> {
    protected view: Quantiles2DView;

    constructor(title: PageTitle,
                page: FullPage,
                protected remoteObject: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                operation: RpcRequest<PartialResult<QuantilesVector>>,
                protected options: ChartOptions) {
        super(options.reusePage ? page : page.dataset.newPage(title, page), operation, "quantiles");
        this.view = new Quantiles2DView(
            this.remoteObject.remoteObjectId, rowCount, schema, samplingRate, this.page);
        this.page.setDataView(this.view);
        this.view.setAxis(axes[0]);
    }

    public onNext(value: PartialResult<QuantilesVector>): void {
        super.onNext(value);
        if (value == null)
            return;
        this.view.updateView(value.data);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}

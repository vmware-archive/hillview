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
import {HeatmapSerialization, IViewSerialization} from "../datasetView";
import {
    AugmentedHistogram,
    FilterDescription,
    Heatmap,
    IColumnDescription,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {DisplayName, SchemaClass} from "../schemaClass";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {HistogramPlot} from "../ui/histogramPlot";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {HtmlString, Resolution} from "../ui/ui";
import {
    formatNumber,
    ICancellable,
    PartialResult,
    reorder,
    saveAs,
    significantDigitsHtml,
} from "../util";
import {AxisData} from "./axisData";
import {NextKReceiver, TableView} from "./tableView";
import {DataRangesCollector, FilterReceiver} from "./dataRangesCollectors";
import {ChartView} from "./chartView";

/**
 * A HeatMapView renders information as a heatmap.
 */
export class HeatmapView extends ChartView {
    protected colorLegend: HeatmapLegendPlot;
    protected summary: HTMLElement;
    protected plot: HeatmapPlot;
    protected showMissingData: boolean = false;  // TODO: enable this
    protected legendSurface: HtmlPlottingSurface;
    protected xHistoSurface: PlottingSurface;
    protected xHistoPlot: HistogramPlot;
    protected heatmap: Heatmap;
    protected xPoints: number;
    protected yPoints: number;
    protected readonly viewMenu: SubMenu;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected legendDiv: HTMLDivElement;
    protected heatmapDiv: HTMLDivElement;
    protected missingDiv: HTMLDivElement;

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Heatmap");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.viewMenu = new SubMenu([
            {
                text: "refresh",
                action: () => {
                    this.refresh();
                },
                help: "Redraw this view.",
            }, {
                text: "swap axes",
                action: () => this.swapAxes(),
                help: "Draw the heatmap with the same data by swapping the X and Y axes.",
            }, {
                text: "table",
                action: () => this.showTable(),
                help: "View the data underlying this view as a table.",
            }, {
                text: "histogram",
                action: () => this.histogram(),
                help: "Show this data as a two-dimensional histogram.",
            }, {
                text: "group by...",
                action: () => this.groupBy(),
                help: "Group data by a third column.",
            },
        ]);
        this.menu = new TopMenu([
            {
                text: "Export",
                help: "Save the information in this view in a local file.",
                subMenu: new SubMenu([{
                    text: "As CSV",
                    help: "Saves the data in this view in a CSV file.",
                    action: () => this.export(),
                }]),
            },
            {text: "View", help: "Change the way the data is displayed.", subMenu: this.viewMenu},
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.legendDiv = this.makeToplevelDiv();
        this.heatmapDiv = this.makeToplevelDiv();
        this.missingDiv = this.makeToplevelDiv();

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    protected createNewSurfaces(keepColorMap: boolean): void {
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        if (this.surface != null)
            this.surface.destroy();
        if (this.xHistoSurface != null)
            this.xHistoSurface.destroy();

        this.legendSurface = new HtmlPlottingSurface(this.legendDiv, this.page,
            { height: Resolution.legendSpaceHeight * 2 / 3 });
        if (keepColorMap)
            this.colorLegend.setSurface(this.legendSurface);
        else {
            this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
            this.colorLegend.setColorMapChangeEventListener(
                () => this.updateView(this.heatmap, true));
        }

        this.surface = new HtmlPlottingSurface(this.heatmapDiv, this.page,
            { topMargin: 20, leftMargin: Resolution.heatmapLabelWidth });
        this.plot = new HeatmapPlot(this.surface, this.colorLegend, true);

        if (this.showMissingData) {
            this.xHistoSurface = new HtmlPlottingSurface(
                this.missingDiv, this.page, {
                    topMargin: 0,
                    bottomMargin: 16,
                    height: 100
                });
            this.xHistoPlot = new HistogramPlot(this.xHistoSurface);
        }
    }

    public setAxes(xData: AxisData, yData: AxisData): void {
        this.xAxisData = xData;
        this.yAxisData = yData;
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
            case "GAxis":
                return null;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                return this.yAxisData;
        }
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.heatmap == null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        let ranges = [];
        if (eventKind === "XAxis") {
            ranges = [sourceRange, this.yAxisData.dataRange];
        } else if (eventKind === "YAxis") {
            ranges = [this.xAxisData.dataRange, sourceRange];
        } else {
            return;
        }

        const collector = new DataRangesCollector(this,
            this.page, null, this.schema, [0, 0],  // any number of buckets
            [this.xAxisData.description, this.yAxisData.description], this.page.title, {
                chartKind: "Heatmap", exact: this.samplingRate >= 1,
                relative: false, reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public updateView(heatmap: Heatmap, keepColorMap: boolean): void {
        this.createNewSurfaces(keepColorMap);
        if (heatmap == null || heatmap.buckets.length === 0) {
            this.page.reportError("No data to display");
            return;
        }

        this.heatmap = heatmap;
        this.xPoints = heatmap.buckets.length;
        this.yPoints = heatmap.buckets[0].length;
        if (this.yPoints === 0) {
            this.page.reportError("No data to display");
            return;
        }

        // The order of these operations is important
        this.plot.setData(heatmap, this.xAxisData, this.yAxisData, this.schema);
        if (!keepColorMap) {
            this.colorLegend.setData(1, this.plot.getMaxCount());
        }
        this.colorLegend.draw();
        this.plot.draw();
        /*
        let margin = this.plot.labelWidth();
        if (margin > this.surface.leftMargin) {
            this.surface.setMargins(null, null, null, margin);
            this.surface.moveCanvas();
        }
        */
        if (this.showMissingData) {
            const augHist: AugmentedHistogram = {
                histogram: heatmap.histogramMissingX,
                cdfBuckets: null,
                confMins: null,
                confMaxes: null
            }
            
            this.xHistoPlot.setHistogram(augHist, this.samplingRate, this.xAxisData, null,
					 this.page.dataset.isPrivate());
            this.xHistoPlot.draw();
        }

        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(),
            [this.schema.displayName(this.xAxisData.description.name).displayName,
                this.schema.displayName(this.yAxisData.description.name).displayName, "count"], 40);
        let summary = new HtmlString(formatNumber(this.plot.getVisiblePoints()) + " data points");
        if (heatmap.missingData !== 0) {
            summary = summary.appendSafeString(", " + formatNumber(heatmap.missingData) + " missing");
        }
        if (heatmap.histogramMissingX.missingData !== 0) {
            summary = summary.appendSafeString(
                ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate");
        }
        if (heatmap.histogramMissingY.missingData !== 0) {
            summary = summary.appendSafeString(
                ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate");
        }
        summary = summary.appendSafeString(", " + formatNumber(this.plot.getDistinct()) + " distinct dots");
        if (this.samplingRate < 1.0) {
            summary = summary.appendSafeString(", sampling rate ").append(significantDigitsHtml(this.samplingRate));
        }
        summary.setInnerHtml(this.summary);
    }

    public serialize(): IViewSerialization {
        const ser = super.serialize();
        const result: HeatmapSerialization = {
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.yAxisData.bucketCount,
            ...ser,
        };
        return result;
    }

    public static reconstruct(ser: HeatmapSerialization, page: FullPage): IDataView {
        if (ser.columnDescription0 == null || ser.columnDescription1 == null ||
            ser.samplingRate == null || ser.schema == null ||
            ser.xBucketCount == null || ser.yBucketCount == null) {
            return null;
        }
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        const hv = new HeatmapView(ser.remoteObjectId, ser.rowCount, schema, ser.samplingRate, page);
        hv.setAxes(new AxisData(ser.columnDescription0, null), new AxisData(ser.columnDescription1, null));
        return hv;
    }

    // Draw this as a 2-D histogram
    public histogram(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false,
            relative: false,
            chartKind: "2DHistogram",
            exact: true
        }));
    }

    public groupBy(): void {
        const columns: DisplayName[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name !== this.xAxisData.description.name
                && col.name !== this.yAxisData.description.name) {
                columns.push(this.schema.displayName(col.name));
            }
        }
        if (columns.length === 0) {
            this.page.reportError("No acceptable columns found");
            return;
        }

        const dialog = new Dialog("Choose column", "Select a column to group on.");
        dialog.addColumnSelectField("column", "column", columns, null,
            "The column that will be used to group on.");
        dialog.setAction(() => this.showTrellis(dialog.getColumnName("column")));
        dialog.show();
    }

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "heatmap.csv";
        saveAs(fileName, lines.join("\n"));
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [
            JSON.stringify(this.xAxisData.getDisplayNameString(this.schema) + "_range") + "," +
            JSON.stringify(this.xAxisData.getDisplayNameString(this.schema)) + "," +
            JSON.stringify(this.yAxisData.getDisplayNameString(this.schema) + "_range") + "," +
            JSON.stringify(this.yAxisData.getDisplayNameString(this.schema)) + "," + "count"];
        for (let x = 0; x < this.heatmap.buckets.length; x++) {
            const data = this.heatmap.buckets[x];
            const bdx = JSON.stringify(this.xAxisData.bucketDescription(x, 0));
            for (let y = 0; y < data.length; y++) {
                if (data[y] === 0) {
                    continue;
                }
                const bdy = JSON.stringify(this.yAxisData.bucketDescription(y, 0));
                const line = bdx + "," + bdy + "," + data[y];
                lines.push(line);
            }
        }
        return lines;
    }

    private showTrellis(colName: DisplayName): void {
        const groupBy = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.xAxisData.description,
                                           this.yAxisData.description, groupBy];
        const rr = this.createDataRangesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: "TrellisHeatmap", exact: true
        }));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(
                title,
                [this.xAxisData.description, this.yAxisData.description],
                this.schema, [0, 0], page, operation, this.dataset, null, 
                { exact: true, chartKind: "Heatmap",
                    reusePage: false, relative: false, });
        };
    }

    // show the table corresponding to the data in the heatmap
    public showTable(): void {
        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true,
        }, {
            columnDescription: this.yAxisData.description,
            isAscending: true,
        }]);
        const page = this.dataset.newPage(new PageTitle("Table"), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page, null);
        page.setDataView(table);
        table.schema = this.schema;
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }

    public swapAxes(): void {
        const cds: IColumnDescription[] = [
            this.yAxisData.description, this.xAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            chartKind: "Heatmap",
            exact: true,
            relative: true,
            reusePage: true
        }));
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.xAxisData.bucketCount, this.yAxisData.bucketCount],
            cds, this.page.title, {
            reusePage: true,
            relative: false,
            chartKind: "Heatmap",
            exact: true
        }));
    }

    public resize(): void {
        if (this.heatmap == null)
            return;
        this.updateView(this.heatmap, true);
    }

    protected onMouseMove(): void {
        if (this.xAxisData.axis == null) {
            // not yet setup
            return;
        }

        const position = d3mouse(this.surface.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];
        const xs = this.xAxisData.invert(mouseX);
        const ys = this.yAxisData.invert(mouseY);

        const value = this.plot.getCount(mouseX, mouseY);
        this.pointDescription.update([xs, ys, value.toString()], mouseX, mouseY);
    }

    protected dragStart(): void {
        this.dragStartRectangle();
    }

    protected dragMove(): boolean {
        return this.dragMoveRectangle();
    }

    protected dragEnd(): boolean {
        if (!super.dragEnd())
            return false;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
        return true;
    }

    /**
     * Selection has been completed.  The mouse coordinates are within the canvas.
     */
    private selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        if (this.xAxisData.axis == null ||
            this.yAxisData.axis == null) {
            return;
        }

        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        yl -= this.surface.topMargin;
        yr -= this.surface.topMargin;
        [xl, xr] = reorder(xl, xr);
        [yr, yl] = reorder(yl, yr);   // y coordinates are in reverse

        const xRange: FilterDescription = {
            min: this.xAxisData.invertToNumber(xl),
            max: this.xAxisData.invertToNumber(xr),
            minString: this.xAxisData.invert(xl),
            maxString: this.xAxisData.invert(xr),
            cd: this.xAxisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const yRange: FilterDescription = {
            min: this.yAxisData.invertToNumber(yl),
            max: this.yAxisData.invertToNumber(yr),
            minString: this.yAxisData.invert(yl),
            maxString: this.yAxisData.invert(yr),
            cd: this.yAxisData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilter2DRequest(xRange, yRange);
        const renderer = new FilterReceiver(
            new PageTitle(
                "Filtered on " + this.xAxisData.getDisplayNameString(this.schema) + " and " +
                this.yAxisData.getDisplayNameString(this.schema)),
            [this.xAxisData.description, this.yAxisData.description],
            this.schema, [0, 0], this.page, rr, this.dataset, [xRange, yRange], {
            exact: this.samplingRate >= 1, chartKind: "Heatmap",
            relative: false, reusePage: false
        });
        rr.invoke(renderer);
    }
}

/**
 * Renders a heatmap
 */
export class HeatmapReceiver extends Receiver<Heatmap> {
    protected heatmap: HeatmapView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axisData: AxisData[],
                protected samplingRate: number,
                operation: ICancellable<Heatmap>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset.newPage(title, page), operation, "histogram");
        this.heatmap = new HeatmapView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.samplingRate, this.page);
        this.heatmap.setAxes(axisData[0], axisData[1]);
        this.page.setDataView(this.heatmap);
    }

    public onNext(value: PartialResult<Heatmap>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }

        this.heatmap.updateView(value.data, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.heatmap.updateCompleted(this.elapsedMilliseconds());
    }
}

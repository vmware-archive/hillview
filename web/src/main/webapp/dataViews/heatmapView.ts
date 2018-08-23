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
import {HeatmapSerialization, IViewSerialization} from "../datasetView";
import {
    CombineOperators,
    FilterDescription,
    Heatmap,
    IColumnDescription,
    RecordOrder,
    RemoteObjectId,
} from "../javaBridge";
import {Receiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BigTableView, TableTargetAPI, ZipReceiver} from "../tableTarget";
import {IDataView} from "../ui/dataview";
import {Dialog} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {HistogramPlot} from "../ui/histogramPlot";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {ChartKind, D3SvgElement, Point, Resolution} from "../ui/ui";
import {
    formatNumber,
    ICancellable,
    PartialResult,
    reorder,
    saveAs,
    significantDigits,
} from "../util";
import {AxisData} from "./axisData";
import {Filter2DReceiver, MakeHistogramOrHeatmap} from "./histogram2DView";
import {HistogramViewBase} from "./histogramViewBase";
import {NextKReceiver, TableView} from "./tableView";
import {DataRangesCollector} from "./dataRangesCollectors";

/**
 * A HeatMapView renders information as a heatmap.
 */
export class HeatmapView extends BigTableView {
    protected dragging: boolean;
    /**
     * Coordinates of mouse within canvas.
     */
    private selectionOrigin: Point;
    private selectionRectangle: D3SvgElement;
    protected colorLegend: HeatmapLegendPlot;
    protected summary: HTMLElement;
    private moved: boolean;
    protected pointDescription: TextOverlay;
    protected surface: PlottingSurface;
    protected plot: HeatmapPlot;
    protected showMissingData: boolean = false;  // TODO: enable this
    protected xHistoSurface: PlottingSurface;
    protected xHistoPlot: HistogramPlot;
    protected heatmap: Heatmap;
    protected xPoints: number;
    protected yPoints: number;
    private readonly menu: TopMenu;
    protected readonly viewMenu: SubMenu;
    protected xData: AxisData;
    protected yData: AxisData;

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Heatmap");
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = (e) => this.keyDown(e);
        this.dragging = false;
        this.moved = false;
        this.viewMenu = new SubMenu([
            {
                text: "refresh",
                action: () => {
                    this.refresh();
                },
                help: "Redraw this view.",
            },
            {
                text: "swap axes",
                action: () => {
                    this.swapAxes();
                },
                help: "Draw the heatmap with the same data by swapping the X and Y axes.",
            },
            {
                text: "table",
                action: () => {
                    this.showTable();
                },
                help: "View the data underlying this view as a table.",
            },
            {
                text: "histogram",
                action: () => {
                    this.histogram();
                },
                help: "Show this data as a two-dimensional histogram.",
            },
            {
                text: "group by...",
                action: () => {
                    this.groupBy();
                },
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
                    action: () => {
                        this.export();
                    },
                }]),
            },
            {text: "View", help: "Change the way the data is displayed.", subMenu: this.viewMenu},
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(this.menu);
        this.topLevel.tabIndex = 1;

        const legendSurface = new HtmlPlottingSurface(this.topLevel, page);
        // legendSurface.setMargins(0, 0, 0, 0);
        legendSurface.setHeight(Resolution.legendSpaceHeight * 2 / 3);
        this.colorLegend = new HeatmapLegendPlot(legendSurface);
        this.colorLegend.setColorMapChangeEventListener(
            () => this.updateView(this.heatmap, true, 0));

        this.surface = new HtmlPlottingSurface(this.topLevel, page);
        this.surface.setMargins(20, this.surface.rightMargin, this.surface.bottomMargin, this.surface.leftMargin);
        this.plot = new HeatmapPlot(this.surface, this.colorLegend, true);

        if (this.showMissingData) {
            this.xHistoSurface = new HtmlPlottingSurface(this.topLevel, page);
            this.xHistoSurface.setMargins(0, null, 16, null);
            this.xHistoSurface.setHeight(100);
            this.xHistoPlot = new HistogramPlot(this.xHistoSurface);
        }

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    public setAxes(xData: AxisData, yData: AxisData): void {
        this.xData = xData;
        this.yData = yData;
    }

    public updateView(heatmap: Heatmap, keepColorMap: boolean, elapsedMs: number): void {
        this.page.reportTime(elapsedMs);
        if (!keepColorMap) {
            this.colorLegend.clear();
        }
        this.plot.clear();
        if (this.showMissingData) {
            this.xHistoPlot.clear();
        }
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
        this.plot.setData(heatmap, this.xData, this.yData);
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
            this.xHistoPlot.setHistogram(heatmap.histogramMissingX, this.samplingRate, this.xData);
            this.xHistoPlot.draw();
        }

        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        const canvas = this.surface.getCanvas();
        canvas.call(drag)
            .on("mousemove", () => this.onMouseMove())
            .on("mouseenter", () => this.onMouseEnter())
            .on("mouseleave", () => this.onMouseLeave());
        this.selectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("stroke", "red")
            .attr("width", 0)
            .attr("height", 0);

        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(),
            [this.xData.description.name, this.yData.description.name, "count"], 40);
        this.pointDescription.show(false);
        let summary = formatNumber(this.plot.getVisiblePoints()) + " data points";
        if (heatmap.missingData !== 0) {
            summary += ", " + formatNumber(heatmap.missingData) + " missing";
        }
        if (heatmap.histogramMissingX.missingData !== 0) {
            summary += ", " + formatNumber(heatmap.histogramMissingX.missingData) + " missing Y coordinate";
        }
        if (heatmap.histogramMissingY.missingData !== 0) {
            summary += ", " + formatNumber(heatmap.histogramMissingY.missingData) + " missing X coordinate";
        }
        summary += ", " + formatNumber(this.plot.getDistinct()) + " distinct dots";
        if (this.samplingRate < 1.0) {
            summary += ", sampling rate " + significantDigits(this.samplingRate);
        }
        this.summary.innerHTML = summary;
    }

    public serialize(): IViewSerialization {
        const ser = super.serialize();
        const result: HeatmapSerialization = {
            samplingRate: this.samplingRate,
            columnDescription0: this.xData.description,
            columnDescription1: this.yData.description,
            ...ser,
        };
        return result;
    }

    public static reconstruct(ser: HeatmapSerialization, page: FullPage): IDataView {
        const samplingRate: number = ser.samplingRate;
        const cd0: IColumnDescription = ser.columnDescription0;
        const cd1: IColumnDescription = ser.columnDescription1;
        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        if (cd0 == null || cd1 == null || samplingRate == null || schema == null) {
            return null;
        }
        const cds = [cd0, cd1];

        const hv = new HeatmapView(ser.remoteObjectId, ser.rowCount, schema, samplingRate, page);
        const buckets = HistogramViewBase.maxHeatmapBuckets(page);
        const rr = hv.getDataRanges(cds, buckets);
        rr.invoke(new DataRangesCollector(hv, hv.page, rr, schema, 0, cds, null, {
            reusePage: true,
            relative: false,
            chartKind: ChartKind.Heatmap,
            exact: samplingRate >= 1
        }));
        return hv;
    }

    // Draw this as a 2-D histogram
    public histogram(): void {
        const buckets = HistogramViewBase.maxHistogram2DBuckets(this.page);
        const cds = [this.xData.description, this.yData.description];
        const rr = this.getDataRanges(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: false,
            relative: false,
            chartKind: ChartKind.Histogram,
            exact: true
        }));
    }

    public groupBy(): void {
        const columns: string[] = [];
        for (let i = 0; i < this.schema.length; i++) {
            const col = this.schema.get(i);
            if (col.name !== this.xData.description.name
                && col.name !== this.yData.description.name) {
                columns.push(this.schema.displayName(col.name));
            }
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

    public export(): void {
        const lines: string[] = this.asCSV();
        const fileName = "heatmap.csv";
        saveAs(fileName, lines.join("\n"));
        this.page.reportError("Check the downloads folder for a file named '" + fileName + "'");
    }

    /**
     * Convert the data to text.
     * @returns {string[]}  An array of lines describing the data.
     */
    public asCSV(): string[] {
        const lines: string[] = [
            JSON.stringify(this.xData.description.name + "_min") + "," +
            JSON.stringify(this.xData.description.name + "_max") + "," +
            JSON.stringify(this.xData.description.name) + "," +
            JSON.stringify(this.yData.description.name + "_min") + "," +
            JSON.stringify(this.yData.description.name + "_max") + "," +
            JSON.stringify(this.yData.description.name) + "," +
                "count",
        ];
        for (let x = 0; x < this.heatmap.buckets.length; x++) {
            const data = this.heatmap.buckets[x];
            const bx = this.xData.boundaries(x);
            const bdx = JSON.stringify(this.xData.bucketDescription(x));
            for (let y = 0; y < data.length; y++) {
                if (data[y] === 0) {
                    continue;
                }
                const by = this.yData.boundaries(y);
                const bdy = JSON.stringify(this.yData.bucketDescription(y));
                const line = bx[0].toString() + "," + bx[1].toString() + "," + bdx + "," +
                    by[0].toString() + "," + by[1].toString() + "," + bdy + "," + data[y];
                lines.push(line);
            }
        }
        return lines;
    }

    private showTrellis(colName: string) {
        const groupBy = this.schema.findByDisplayName(colName);
        const cds: IColumnDescription[] = [this.xData.description,
                                           this.yData.description, groupBy];
        const buckets = HistogramViewBase.maxTrellis3DBuckets(this.page);
        const rr = this.getDataRanges(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: false, relative: false,
            chartKind: ChartKind.TrellisHeatmap, exact: true
        }));
    }

    // combine two views according to some operation
    public combine(how: CombineOperators): void {
        const r = this.dataset.getSelected();
        if (r.first == null) {
            return;
        }

        const rr = this.createZipRequest(r.first);
        const renderer = (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new MakeHistogramOrHeatmap(
                page, operation,
                [this.xData.description, this.yData.description],
                this.rowCount, this.schema,
                { exact: this.samplingRate >= 1, chartKind: ChartKind.Heatmap,
                    reusePage: false, relative: false, },
                this.dataset);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.dataset, renderer));
    }

    // show the table corresponding to the data in the heatmap
    public showTable(): void {
        const order =  new RecordOrder([ {
            columnDescription: this.xData.description,
            isAscending: true,
        }, {
            columnDescription: this.yData.description,
            isAscending: true,
        }]);
        const page = this.dataset.newPage("Table", this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, page);
        page.setDataView(table);
        table.schema = this.schema;
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(page, table, rr, false, order, null));
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code === "Escape") {
            this.cancelDrag();
        }
    }

    protected cancelDrag() {
        this.dragging = false;
        this.moved = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    public swapAxes(): void {
        const cds: IColumnDescription[] = [
            this.yData.description, this.xData.description];
        const buckets = HistogramViewBase.maxHeatmapBuckets(this.page);
        const rr = this.getDataRanges(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            chartKind: ChartKind.Heatmap,
            exact: true,
            relative: true,
            reusePage: true
        }));
    }

    public refresh(): void {
        if (this.heatmap == null)
            return;
        this.updateView(this.heatmap, true, 0);
    }

    protected onMouseEnter(): void {
        if (this.pointDescription != null) {
            this.pointDescription.show(true);
        }
    }

    protected onMouseLeave(): void {
        if (this.pointDescription != null) {
            this.pointDescription.show(false);
        }
    }

    public onMouseMove(): void {
        if (this.xData.axis == null) {
            // not yet setup
            return;
        }

        const position = d3mouse(this.surface.getChart().node());
        const mouseX = position[0];
        const mouseY = position[1];
        const xs = this.xData.invert(mouseX);
        const ys = this.yData.invert(mouseY);

        const value = this.plot.getCount(mouseX, mouseY);
        this.pointDescription.update([xs, ys, value.toString()], mouseX, mouseY);
    }

    public dragStart(): void {
        this.dragging = true;
        this.moved = false;
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    public dragMove(): void {
        this.onMouseMove();
        if (!this.dragging) {
            return;
        }
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let oy = this.selectionOrigin.y;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        let width = x - ox;
        let height = y - oy;

        if (width < 0) {
            ox = x;
            width = -width;
        }
        if (height < 0) {
            oy = y;
            height = -height;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", oy)
            .attr("width", width)
            .attr("height", height);
    }

    public dragEnd(): void {
        if (!this.dragging || !this.moved) {
            return;
        }
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        const y = position[1];
        this.selectionCompleted(this.selectionOrigin.x, x, this.selectionOrigin.y, y);
    }

    /**
     * Selection has been completed.  The mouse coordinates are within the canvas.
     */
    public selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        if (this.xData.axis == null ||
            this.yData.axis == null) {
            return;
        }

        xl -= this.surface.leftMargin;
        xr -= this.surface.leftMargin;
        yl -= this.surface.topMargin;
        yr -= this.surface.topMargin;
        [xl, xr] = reorder(xl, xr);
        [yr, yl] = reorder(yl, yr);   // y coordinates are in reverse

        const xRange: FilterDescription = {
            min: this.xData.invertToNumber(xl),
            max: this.xData.invertToNumber(xr),
            minString: this.xData.invert(xl),
            maxString: this.xData.invert(xr),
            cd: this.xData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const yRange: FilterDescription = {
            min: this.yData.invertToNumber(yl),
            max: this.yData.invertToNumber(yr),
            minString: this.yData.invert(yl),
            maxString: this.yData.invert(yr),
            cd: this.yData.description,
            complement: d3event.sourceEvent.ctrlKey,
        };
        const rr = this.createFilter2DRequest(xRange, yRange);
        const renderer = new Filter2DReceiver(
            "Filtered on " + this.xData.description.name + " and " +
            this.yData.description.name,
            this.xData.description, this.yData.description,
            this.schema, 0, this.page, rr, this.dataset, {
            exact: this.samplingRate >= 1, chartKind: ChartKind.Heatmap,
            relative: false, reusePage: false
        });
        rr.invoke(renderer);
    }
}

/**
 * Renders a heatmap
 */
export class HeatmapRenderer extends Receiver<Heatmap> {
    protected heatmap: HeatmapView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axisData: AxisData[],
                protected samplingRate: number,
                operation: ICancellable<Heatmap>,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Heatmap (" + schema.displayName(axisData[0].description.name) + ", " +
                schema.displayName(axisData[1].description.name) + ")", page),
            operation, "histogram");

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

        this.heatmap.updateView(value.data, false, this.elapsedMilliseconds());
    }
}

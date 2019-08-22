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

import {
    FilterDescription,
    Heatmap,
    Heatmap3D,
    IColumnDescription,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {BaseReceiver, TableTargetAPI} from "../tableTarget";
import {DragEventKind, FullPage, PageTitle} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {Resolution} from "../ui/ui";
import {AxisData, AxisKind} from "./axisData";
import {
    FilterReceiver,
    DataRangesCollector,
    TrellisShape,
    TrellisLayoutComputation
} from "./dataRangesCollectors";
import {Receiver, RpcRequest} from "../rpc";
import {ICancellable, PartialResult, reorder} from "../util";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {IViewSerialization, TrellisHeatmapSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {TextOverlay} from "../ui/textOverlay";
import {TrellisChartView} from "./trellisChartView";
import {NextKReceiver, TableView} from "./tableView";

/**
 * A Trellis plot containing multiple heatmaps.
 */
export class TrellisHeatmapView extends TrellisChartView {
    private colorLegend: HeatmapLegendPlot;
    private legendSurface: HtmlPlottingSurface;
    private readonly legendDiv: HTMLDivElement;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected hps: HeatmapPlot[];
    protected heatmaps: Heatmap3D;

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                protected shape: TrellisShape,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, shape, page, "TrellisHeatmap");
        this.xAxisData = null;
        this.yAxisData = null;
        this.hps = [];
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");

        this.menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => this.refresh(),
                    help: "Redraw this view.",
                }, {
                text: "swap axes",
                    action: () => this.swapAxes(),
                    help: "Swap the X and Y axes of all plots."
                }, { text: "table",
                    action: () => this.showTable(),
                    help: "Show the data underlying this view in a tabular view."
                }, { text: "histogram",
                        action: () => this.histogram(),
                        help: "Show this data as a Trellis plot of two-dimensional histograms."
                }, { text: "# groups",
                        action: () => this.changeGroups(),
                        help: "Change the number of groups."
                    }
                ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);
        this.page.setMenu(this.menu);
        this.legendDiv = this.makeToplevelDiv();
    }

    public static reconstruct(ser: TrellisHeatmapSerialization, page: FullPage): IDataView {
        if (ser.columnDescription0 == null || ser.columnDescription1 == null ||
            ser.samplingRate == null || ser.schema == null ||
            ser.xBucketCount == null || ser.yBucketCount == null) {
            return null;
        }
        const shape = TrellisChartView.deserializeShape(ser, page);
        if (shape == null)
            return null;

        const schema: SchemaClass = new SchemaClass([]).deserialize(ser.schema);
        const hv = new TrellisHeatmapView(ser.remoteObjectId, ser.rowCount, schema, shape, ser.samplingRate, page);
        hv.setAxes(new AxisData(ser.columnDescription0, null),
            new AxisData(ser.columnDescription1, null),
            new AxisData(ser.groupByColumn, null));
        return hv;
    }

    public serialize(): IViewSerialization {
        const ser: TrellisHeatmapSerialization = {
            ...super.serialize(),
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.yAxisData.bucketCount,
            groupByColumn: this.groupByAxisData.description,
            xWindows: this.shape.xNum,
            yWindows: this.shape.yNum,
            groupByBucketCount: this.groupByAxisData.bucketCount
        };
        return ser;
    }

    protected createNewSurfaces(): void {
        if (this.surface != null)
            this.surface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        this.legendSurface = new HtmlPlottingSurface(this.legendDiv, this.page, {
            height: Resolution.legendSpaceHeight });
        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
        this.hps = [];
        this.createAllSurfaces((surface) => {
            const hp = new HeatmapPlot(surface, this.colorLegend, false);
            this.hps.push(hp);
        });
    }

    public getAxisData(event: DragEventKind): AxisData | null {
        switch (event) {
            case "Title":
                return null;
            case "GAxis":
                return this.groupByAxisData;
            case "XAxis":
                return this.xAxisData;
            case "YAxis":
                return this.yAxisData;
        }
    }

    public setAxes(xAxisData: AxisData,
                   yAxisData: AxisData,
                   groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.yAxisData = yAxisData;
        this.groupByAxisData = groupByAxisData;
    }

    protected replaceAxis(pageId: string, eventKind: DragEventKind): void {
        if (this.heatmaps == null)
            return;
        const sourceRange = this.getSourceAxisRange(pageId, eventKind);
        if (sourceRange === null)
            return;

        let ranges = [];
        if (eventKind === "XAxis") {
            ranges = [sourceRange, this.yAxisData.dataRange, this.groupByAxisData.dataRange];
        } else if (eventKind === "YAxis") {
            ranges = [this.xAxisData.dataRange, sourceRange, this.groupByAxisData.dataRange];
        } else if (eventKind === "GAxis") {
            ranges = [this.xAxisData.dataRange, this.yAxisData.dataRange, sourceRange];
        } else {
            return;
        }

        const collector = new DataRangesCollector(this,
            this.page, null, this.schema, [0, 0, 0],  // any number of buckets
            [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description],
            this.page.title, {
                chartKind: "TrellisHeatmap", exact: this.samplingRate >= 1,
                relative: false, reusePage: true
            });
        collector.run(ranges);
        collector.finished();
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [this.xAxisData.bucketCount, this.yAxisData.bucketCount, this.shape.bucketCount], cds, null, {
                reusePage: true, relative: false,
                chartKind: "TrellisHeatmap", exact: true
            }));
    }

    public resize(): void {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        this.shape = TrellisLayoutComputation.resize(chartSize.width, chartSize.height, this.shape);
        this.updateView(this.heatmaps);
    }

    protected doChangeGroups(groupCount: number): void {
        if (groupCount == null) {
            this.page.reportError("Illegal group count");
            return;
        }
        if (groupCount === 1) {
            const cds = [this.xAxisData.description, this.yAxisData.description];
            const rr = this.createDataRangesRequest(cds, this.page, "Heatmap");
            rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
                [0], cds, null, {
                    reusePage: true, relative: false,
                    chartKind: "Heatmap", exact: this.samplingRate >= 1
                }));
        } else {
            const cds = [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
            const rr = this.createDataRangesRequest(cds, this.page, "TrellisHeatmap");
            rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
                [0, 0, groupCount], cds, null, {
                    reusePage: true, relative: false,
                    chartKind: "TrellisHeatmap", exact: this.samplingRate >= 1
                }));
        }
    }

    public histogram(): void {
        const cds: IColumnDescription[] = [
            this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0, 0], cds, null, {
                chartKind: "Trellis2DHistogram",
                exact: true,
                relative: false,
                reusePage: false
            }));
    }

    public swapAxes(): void {
        const cds: IColumnDescription[] = [
            this.yAxisData.description, this.xAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataRangesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
                chartKind: "TrellisHeatmap",
                exact: true,
                relative: true,
                reusePage: true
            }));
    }

    public showTable(): void {
        const newPage = this.dataset.newPage(new PageTitle("Table"), this.page);
        const table = new TableView(this.remoteObjectId, this.rowCount, this.schema, newPage, null);
        newPage.setDataView(table);
        table.schema = this.schema;

        const order =  new RecordOrder([ {
            columnDescription: this.xAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.yAxisData.description,
            isAscending: true
        }, {
            columnDescription: this.groupByAxisData.description,
            isAscending: true
        }]);
        const rr = table.createNextKRequest(order, null, Resolution.tableRowsOnScreen);
        rr.invoke(new NextKReceiver(newPage, table, rr, false, order, null));
    }

    public updateView(heatmaps: Heatmap3D): void {
        this.createNewSurfaces();
        this.heatmaps = heatmaps;
        if (heatmaps == null || heatmaps.buckets.length === 0) {
            this.page.reportError("No data to display");
            return;
        }

        let max = 0;
        for (let i = 0; i < heatmaps.buckets.length; i++) {
            const buckets = heatmaps.buckets[i];
            const heatmap: Heatmap = {
                buckets: buckets,
                histogramMissingX: null,
                histogramMissingY: null,
                missingData: heatmaps.eitherMissing,
                totalSize: heatmaps.eitherMissing + heatmaps.totalPresent
            };
            const plot = this.hps[i];
            // The order of these operations is important
            plot.setData(heatmap, this.xAxisData, this.yAxisData, this.schema);
            max = Math.max(max, plot.getMaxCount());
        }

        this.colorLegend.setData(1, max);
        this.colorLegend.draw();
        for (const plot of this.hps) {
            plot.draw();
            plot.border(1);
        }

        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom, PlottingSurface.bottomMargin);
        // This axis is only created when the surface is drawn
        this.yAxisData.setResolution(this.shape.size.height, AxisKind.Left, Resolution.heatmapLabelWidth);
        this.drawAxes(this.xAxisData.axis, this.yAxisData.axis);

        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getDisplayNameString(this.schema),
                this.yAxisData.getDisplayNameString(this.schema),
                this.groupByAxisData.getDisplayNameString(this.schema),
                "count"], 40);

        // Axis labels
        const canvas = this.surface.getCanvas();
        canvas.append("text")
            .text(this.schema.displayName(this.yAxisData.description.name))
            .attr("dominant-baseline", "text-before-edge");
        canvas.append("text")
            .text(this.schema.displayName(this.xAxisData.description.name))
            .attr("transform", `translate(${this.surface.getChartWidth() / 2},
                      ${this.surface.getChartHeight() + this.surface.topMargin +
            this.surface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");
    }

    public onMouseMove(): void {
        const mousePosition = this.mousePosition();
        if (mousePosition.plotIndex == null ||
            mousePosition.x < 0 || mousePosition.y < 0) {
            this.pointDescription.show(false);
            return;
        }

        this.pointDescription.show(true);
        const plot = this.hps[mousePosition.plotIndex];
        const xs = this.xAxisData.invert(mousePosition.x);
        const ys = this.yAxisData.invert(mousePosition.y);
        const value = plot.getCount(mousePosition.x, mousePosition.y);
        const group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex, 40);

        // The point description is a child of the canvas, so we use canvas coordinates
        const position = d3mouse(this.surface.getCanvas().node());
        this.pointDescription.update([xs, ys, group, value.toString()], position[0], position[1]);
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new FilterReceiver(
                title,
                [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description],
                this.schema, [0, 0, 0], page, operation, this.dataset, null, {
                    chartKind: "TrellisHeatmap",
                    relative: false,
                    reusePage: false,
                    exact: this.samplingRate >= 1
                });
        };
    }

    protected selectionCompleted(): void {
        const local = this.selectionIsLocal();
        let title: PageTitle;
        let rr: RpcRequest<PartialResult<RemoteObjectId>>;
        if (local != null) {
            const origin = this.canvasToChart(this.selectionOrigin);
            const left = this.position(origin.x, origin.y);
            const end = this.canvasToChart(this.selectionEnd);
            const right = this.position(end.x, end.y);

            const [xl, xr] = reorder(left.x, right.x);
            const [yr, yl] = reorder(left.y, right.y);   // y coordinates are in reverse

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
            rr = this.createFilter2DRequest(xRange, yRange);
            title = new PageTitle("Filtered on " +
                this.schema.displayName(this.xAxisData.description.name) +
                " and " + this.schema.displayName(this.yAxisData.description.name));
            const renderer = new FilterReceiver(title,
                [this.xAxisData.description, this.yAxisData.description,
                    this.groupByAxisData.description],
                this.schema, [0, 0, 0], this.page, rr, this.dataset, [xRange, yRange],
                {
                    chartKind: "TrellisHeatmap", relative: false, reusePage: false,
                    exact: this.samplingRate >= 1
            });
        rr.invoke(renderer);
        } else {
            const filter = this.getGroupBySelectionFilter();
            if (filter == null)
                return;
            rr = this.createFilterRequest(filter);
            title = new PageTitle(
                "Filtered on " + this.groupByAxisData.getDisplayNameString(this.schema));
            const renderer = new FilterReceiver(title,
                [this.xAxisData.description, this.yAxisData.description,
                    this.groupByAxisData.description],
                this.schema, [0, 0, 0], this.page, rr, this.dataset, [filter],
                {
                    chartKind: "TrellisHeatmap", relative: false, reusePage: false,
                    exact: this.samplingRate >= 1
                });
            rr.invoke(renderer);
        }
    }
}

/**
 * Renders a Trellis plot of heatmaps
 */
export class TrellisHeatmapReceiver extends Receiver<Heatmap3D> {
    protected trellisView: TrellisHeatmapView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Heatmap3D>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset.newPage(title, page), operation, "histogram");
        this.trellisView = new TrellisHeatmapView(
            remoteTable.remoteObjectId, rowCount, schema,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1], axes[2]);
        this.page.setDataView(this.trellisView);
    }

    public onNext(value: PartialResult<Heatmap3D>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }
        this.trellisView.updateView(value.data);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.trellisView.updateCompleted(this.elapsedMilliseconds());
    }
}

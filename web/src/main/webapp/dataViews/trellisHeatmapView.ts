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
    Groups,
    IColumnDescription, RangeFilterArrayDescription,
    RemoteObjectId, RowValue
} from "../javaBridge";
import {BaseReceiver, TableTargetAPI} from "../modules";
import {FullPage, PageTitle} from "../ui/fullPage";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {DragEventKind, HtmlString, Resolution} from "../ui/ui";
import {AxisData, AxisKind} from "./axisData";
import {
    NewTargetReceiver,
    DataRangesReceiver,
    TrellisShape,
    TrellisLayoutComputation
} from "./dataRangesReceiver";
import {Receiver} from "../rpc";
import {
    assert,
    assertNever,
    Converters, Exporter,
    GroupsClass, Heatmap,
    ICancellable,
    makeInterval,
    PartialResult,
    reorder, Triple
} from "../util";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {IViewSerialization, TrellisHeatmapSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {TextOverlay} from "../ui/textOverlay";
import {TrellisChartView} from "./trellisChartView";
import {HeatmapLegendPlot} from "../ui/heatmapLegendPlot";
import {saveAs} from "../ui/dialog";
import {TableMeta} from "../ui/receiver";

/**
 * A Trellis plot containing multiple heatmaps.
 */
export class TrellisHeatmapView extends TrellisChartView<Groups<Groups<Groups<number>>>> {
    private colorLegend: HeatmapLegendPlot;
    private legendSurface: HtmlPlottingSurface;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected hps: HeatmapPlot[];
    protected readonly defaultProvenance = "Trellis heatmaps";

    constructor(remoteObjectId: RemoteObjectId,
                meta: TableMeta,
                protected shape: TrellisShape,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, meta, shape, page, "TrellisHeatmap");
        this.hps = [];
        this.menu = new TopMenu([this.exportMenu(),
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => this.refresh(),
                    help: "Redraw this view.",
                }, {
                text: "swap axes",
                    action: () => this.swapAxes(),
                    help: "Swap the X and Y axes of all plots."
                }, { text: "table",
                    action: () => this.showTable(
                        [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description],
                        this.defaultProvenance),
                    help: "Show the data underlying this view in a tabular view."
                }, { text: "histogram",
                        action: () => this.histogram(),
                        help: "Show this data as a Trellis plot of two-dimensional histograms."
                }, { text: "# groups",
                        action: () => this.changeGroups(),
                        help: "Change the number of groups."
                }, {
                    text: "Show/hide regression",
                    action: () => this.toggleRegression(),
                    help: "Show or hide the linear regression line"
                }]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);
        this.page.setMenu(this.menu);
        this.createDiv("legend");
        this.createDiv("chart");
        this.createDiv("summary");
    }

    public static reconstruct(ser: TrellisHeatmapSerialization, page: FullPage): IDataView | null {
        if (ser.columnDescription0 == null || ser.columnDescription1 == null ||
            ser.samplingRate == null || ser.windowCount === null ||
            ser.xBucketCount == null || ser.yBucketCount == null ||
            ser.yRange === null || ser.xRange === null || ser.gRange === null) {
            return null;
        }
        const shape = TrellisChartView.deserializeShape(ser, page);
        if (shape == null)
            return null;

        const args = this.validateSerialization(ser);
        if (args == null)
            return null;
        const hv = new TrellisHeatmapView(ser.remoteObjectId, args, shape, ser.samplingRate, page);
        hv.setAxes(new AxisData(ser.columnDescription0, ser.xRange, ser.xBucketCount),
            new AxisData(ser.columnDescription1, ser.yRange, ser.yBucketCount),
            new AxisData(ser.groupByColumn, ser.gRange, ser.windowCount));
        return hv;
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const ser: TrellisHeatmapSerialization = {
            ...super.serialize(),
            ...this.shape,
            xRange: this.xAxisData.dataRange,
            yRange: this.yAxisData.dataRange,
            gRange: this.groupByAxisData.dataRange,
            samplingRate: this.samplingRate,
            columnDescription0: this.xAxisData.description,
            columnDescription1: this.yAxisData.description,
            xBucketCount: this.xAxisData.bucketCount,
            yBucketCount: this.yAxisData.bucketCount,
            groupByColumn: this.groupByAxisData.description
        };
        return ser;
    }

    public export(): void {
        const lines = Exporter.histogram3DAsCsv(
            this.data, this.getSchema(), [this.xAxisData, this.yAxisData, this.groupByAxisData]);
        const fileName = "trellis-heatmap.csv";
        saveAs(fileName, lines.join("\n"));
    }

    protected createNewSurfaces(keepColorMap: boolean): void {
        if (this.surface != null)
            this.surface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        this.legendSurface = new HtmlPlottingSurface(this.legendDiv!, this.page, {
            height: Resolution.legendSpaceHeight });
        if (keepColorMap)
            this.colorLegend.setSurface(this.legendSurface);
        else {
            this.colorLegend = new HeatmapLegendPlot(
                this.legendSurface, (xl, xr) =>
                    this.legendSelectionCompleted(xl, xr));
            this.colorLegend.setColorMapChangeEventListener(
                () => this.updateView(this.data, true));
        }
        this.hps = [];
        this.createAllSurfaces((surface) => {
            const hp = new HeatmapPlot(
                surface, this.colorLegend.getColorMap(), null, 0,false);
            this.hps.push(hp);
        });
    }

    public legendSelectionCompleted(xl: number, xr: number): void {
        const [min, max] = reorder(this.colorLegend.invert(xl), this.colorLegend.invert(xr));
        const g = new GroupsClass(this.data).map(g => Heatmap.create(g));
        const bitmap = g.map(g => g.bitmap((c) => min <= c && c <= max));
        const filter = g.map(g => g.map((c) => min <= c && c <= max ? c : 0));
        const bucketCount = bitmap.reduce((n, g) => n + g.sum(), 0);
        const pointCount = filter.reduce((n, g) => n + g.sum(), 0);
        const shiftPressed = d3event.sourceEvent.shiftKey;
        assert(this.summary != null);
        this.summary.set("buckets selected", bucketCount);
        this.summary.set("points selected", pointCount);
        this.summary.display();
        if (shiftPressed) {
            this.colorLegend.emphasizeRange(xl, xr);
        }
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
            default:
                assertNever(event);
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
        if (this.data == null)
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

        const receiver = new DataRangesReceiver(this,
            this.page, null, this.meta, [0, 0, 0],  // any number of buckets
            [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description],
            this.page.title, this.defaultProvenance, {
                chartKind: "TrellisHeatmap", exact: this.samplingRate >= 1,
                relative: false, reusePage: true
            });
        receiver.run(ranges);
        receiver.finished();
    }

    public refresh(): void {
        const cds = [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
        const ranges = [this.xAxisData.dataRange, this.yAxisData.dataRange, this.groupByAxisData.dataRange];
        const receiver = new DataRangesReceiver(this,
            this.page, null, this.meta,
            [this.xAxisData.bucketCount, this.yAxisData.bucketCount, this.groupByAxisData.bucketCount],
            cds, this.page.title, null,{
                chartKind: "TrellisHeatmap", exact: this.samplingRate >= 1,
                relative: false, reusePage: true
            });
        receiver.run(ranges);
        receiver.finished();
    }

    public resize(): void {
        const chartSize = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        this.shape = TrellisLayoutComputation.resize(chartSize.width, chartSize.height, this.shape);
        this.updateView(this.data, true);
    }

    protected doChangeGroups(groupCount: number): void {
        if (groupCount == null) {
            this.page.reportError("Illegal group count");
            return;
        }
        if (groupCount === 1) {
            const cds = [this.xAxisData.description, this.yAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "Heatmap");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
                [0], cds, null, "change groups",{
                    reusePage: true, relative: false,
                    chartKind: "Heatmap", exact: this.samplingRate >= 1
                }));
        } else {
            const cds = [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
            const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHeatmap");
            rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
                [0, 0, groupCount], cds, null, "change groups", {
                    reusePage: true, relative: false,
                    chartKind: "TrellisHeatmap", exact: this.samplingRate >= 1
                }));
        }
    }

    public histogram(): void {
        const cds: IColumnDescription[] = [
            this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "Trellis2DHistogram");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0, 0], cds, null, this.defaultProvenance, {
                chartKind: "Trellis2DHistogram",
                exact: true,
                relative: false,
                reusePage: false
            }));
    }

    public swapAxes(): void {
        const cds: IColumnDescription[] = [
            this.yAxisData.description, this.xAxisData.description, this.groupByAxisData.description];
        const rr = this.createDataQuantilesRequest(cds, this.page, "TrellisHeatmap");
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.meta,
            [0, 0], cds, null, "swap axes", {
                chartKind: "TrellisHeatmap",
                exact: true,
                relative: true,
                reusePage: true
            }));
    }

    public toggleRegression(): void {
        for (const hp of this.hps)
            hp.toggleRegression();
    }

    public updateView(histogram3d: Groups<Groups<Groups<number>>>, keepColorMap: boolean): void {
        this.createNewSurfaces(keepColorMap);
        this.data = histogram3d;
        if (histogram3d == null || histogram3d.perBucket.length === 0) {
            this.page.reportError("No data to display");
            return;
        }

        let max = 0;
        for (let i = 0; i < histogram3d.perBucket.length; i++) {
            const buckets = histogram3d.perBucket[i];
            const heatmap: Triple<Groups<Groups<number>>, Groups<Groups<number>> | null, Groups<Groups<RowValue[]>> | null> =
                { first: buckets, second: null, third: null };
            const plot = this.hps[i];
            // The order of these operations is important
            plot.setData(heatmap, this.xAxisData, this.yAxisData, null, this.getSchema(), 2, this.isPrivate());
            max = Math.max(max, plot.getMaxCount());
        }
        if (this.shape.missingBucket) {
            const buckets = histogram3d.perMissing;
            const heatmap: Triple<Groups<Groups<number>>, Groups<Groups<number>> | null, Groups<Groups<RowValue[]>>| null> =
                { first: buckets, second: null, third: null };
            const plot = this.hps[histogram3d.perBucket.length];
            // The order of these operations is important
            plot.setData(heatmap, this.xAxisData, this.yAxisData, null, this.getSchema(), 2, this.isPrivate());
            max = Math.max(max, plot.getMaxCount());
        }

        this.colorLegend.setData(max);
        this.colorLegend.draw();
        for (const plot of this.hps) {
            plot.draw();
            plot.border(1);
        }

        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom, PlottingSurface.bottomMargin);
        // This axis is only created when the surface is drawn
        this.yAxisData.setResolution(this.shape.size.height, AxisKind.Left, Resolution.heatmapLabelWidth);
        this.drawAxes(this.xAxisData.axis!, this.yAxisData.axis!);

        this.setupMouse();
        assert(this.surface != null);
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            [this.xAxisData.getName()!,
                this.yAxisData.getName()!,
                this.groupByAxisData.getName()!,
                "count"], 40);

        // Axis labels
        const canvas = this.surface.getCanvas();
        canvas.append("text")
            .text(this.yAxisData.description.name)
            .attr("dominant-baseline", "text-before-edge");
        canvas.append("text")
            .text(this.xAxisData.description.name)
            .attr("transform", `translate(${this.surface.getChartWidth() / 2},
                      ${this.surface.getChartHeight() + this.surface.topMargin +
            this.surface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");
        this.standardSummary();
        assert(this.summary != null);
        this.summary.setString("pixel width", new HtmlString(this.xAxisData.barWidth()));
        this.summary.setString("pixel height", new HtmlString(this.yAxisData.barWidth()));
        this.summary.display();
    }

    public onMouseMove(): void {
        const mousePosition = this.checkMouseBounds();
        if (mousePosition == null || mousePosition.plotIndex == null)
            return;

        this.pointDescription!.show(true);
        const plot = this.hps[mousePosition.plotIndex];
        const xs = this.xAxisData.invert(mousePosition.x);
        const ys = this.yAxisData.invert(mousePosition.y);
        const value = plot.getCount(mousePosition.x, mousePosition.y);
        const group = this.groupByAxisData.bucketDescription(mousePosition.plotIndex, 40);

        // The point description is a child of the canvas, so we use canvas coordinates
        const position = d3mouse(this.surface!.getCanvas().node());
        this.pointDescription!.update([xs, ys, group, makeInterval(value)], position[0], position[1]);
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new NewTargetReceiver(title,
                [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description],
                this.meta, [0, 0, 0], page, operation, this.dataset, {
                chartKind: "TrellisHeatmap",
                relative: false,
                reusePage: false,
                exact: this.samplingRate >= 1
            });
        };
    }

    protected filter(filter: RangeFilterArrayDescription): void {
        const rr = this.createFilterRequest(filter);
        const title = new PageTitle(this.page.title.format, Converters.filterArrayDescription(filter));
        const renderer = new NewTargetReceiver(title,
            [this.xAxisData.description, this.yAxisData.description, this.groupByAxisData.description],
            this.meta, [0, 0, 0], this.page, rr, this.dataset, {
                chartKind: "TrellisHeatmap", relative: false, reusePage: false,
                exact: this.samplingRate >= 1
            });
        rr.invoke(renderer);
    }

    protected selectionCompleted(): void {
        const local = this.selectionIsLocal();
        if (local != null) {
            const origin = this.canvasToChart(this.selectionOrigin!);
            const left = this.position(origin.x, origin.y);
            assert(left != null);
            const end = this.canvasToChart(this.selectionEnd!);
            const right = this.position(end.x, end.y);
            assert(right != null);
            const xRange = this.xAxisData.getFilter(left.x, right.x);
            const yRange = this.yAxisData.getFilter(left.y, right.y);
            const f: RangeFilterArrayDescription = {
                filters: [xRange, yRange],
                complement: d3event.sourceEvent.ctrlKey
            }
            //title = new PageTitle(this.page.title.format,
            //    Converters.filterDescription(xRange) + " and " + Converters.filterDescription(yRange));
            this.filter(f);
        } else {
            const filter = this.getGroupBySelectionFilter();
            if (filter == null)
                return;
            this.filter(filter);
        }
    }
}

/**
 * Renders a Trellis plot of heatmaps
 */
export class TrellisHeatmapReceiver extends Receiver<Groups<Groups<Groups<number>>>> {
    protected trellisView: TrellisHeatmapView;

    constructor(title: PageTitle,
                page: FullPage,
                remoteTable: TableTargetAPI,
                protected meta: TableMeta,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Groups<Groups<Groups<number>>>>,
                protected reusePage: boolean) {
        super(reusePage ? page : page.dataset!.newPage(title, page), operation, "histogram");
        this.trellisView = new TrellisHeatmapView(
            remoteTable.remoteObjectId, meta,
            this.shape, this.samplingRate, this.page);
        this.trellisView.setAxes(axes[0], axes[1], axes[2]);
    }

    public onNext(value: PartialResult<Groups<Groups<Groups<number>>>>): void {
        super.onNext(value);
        if (value == null) {
            return;
        }
        this.trellisView.updateView(value.data, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.trellisView.updateCompleted(this.elapsedMilliseconds());
    }
}

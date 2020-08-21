/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {BucketsInfo, Groups, HistogramRequestInfo, RemoteObjectId} from "../javaBridge";
import {BaseReceiver, ChartView} from "../modules";
import {FullPage, PageTitle} from "../ui/fullPage";
import {
    assert,
    Converters, Heatmap,
    histogram2DAsCsv,
    ICancellable,
    makeInterval,
    PartialResult, reorder,
    truncate,
    zip
} from "../util";
import {DisplayName} from "../schemaClass";
import {RpcRequest} from "../rpc";
import {CommonArgs, ReceiverCommon, ReceiverCommonArgs} from "../ui/receiver";
import {SubMenu, TopMenu} from "../ui/menu";
import {CorrelationHeatmapSerialization, IViewSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {Point, Resolution} from "../ui/ui";
import {ColorMapKind, HeatmapLegendPlot} from "../ui/heatmapLegendPlot";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {AxisData, AxisKind} from "./axisData";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {TextOverlay} from "../ui/textOverlay";
import {DataRangesReceiver, NewTargetReceiver} from "./dataRangesReceiver";
import {saveAs} from "../ui/dialog";

export class CorrelationHeatmapView extends ChartView<Groups<Groups<number>>[]> {
    private legendSurface: HtmlPlottingSurface;
    private colorLegend: HeatmapLegendPlot;
    protected hps: HeatmapPlot[];
    protected surfaces: PlottingSurface[];
    protected xAxes: AxisData[];
    protected yAxes: AxisData[];
    protected chartSize: number;
    protected headerHeight: number;
    /**
     * Coordinates of each surface within the canvas.
     */
    protected coordinates: Point[];

    constructor(args: CommonArgs, protected histoArgs: HistogramRequestInfo,
                protected ranges: BucketsInfo[], page: FullPage) {
        super(args.remoteObject.remoteObjectId, args.rowCount, args.schema, page, "CorrelationHeatmaps")
        this.menu = new TopMenu([this.exportMenu(),
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                    { text: "refresh",
                        action: () => this.refresh(),
                        help: "Redraw this view.",
                    }
                ]) },
            this.dataset.combineMenu(this, page.pageId),
        ]);
        this.page.setMenu(this.menu);
        this.createDiv("legend");
        this.createDiv("chart");
        this.createDiv("summary");
        this.xAxes = zip(this.histoArgs.histos, this.ranges,
            (h, r) => new AxisData(h.cd, r, h.bucketCount));
        // exact same code, but the resolution will be different
        this.yAxes = zip(this.histoArgs.histos, this.ranges,
            (h, r) => new AxisData(h.cd, r, h.bucketCount));
    }

    public static reconstruct(ser: CorrelationHeatmapSerialization, page: FullPage): IDataView | null {
        const args = this.validateSerialization(ser);
        if (args == null || ser.histoArgs == null || ser.ranges == null)
            return null;
        return new CorrelationHeatmapView(args, ser.histoArgs, ser.ranges, page);
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
        const result: CorrelationHeatmapSerialization = {
            ...super.serialize(),
            histoArgs: this.histoArgs,
            ranges: this.ranges
        };
        return result;
    }

    protected export(): void {
        let result: string[] = [];
        let xAxis = 1;
        let yAxis = 0;
        for (const h of this.data) {
            const lines = histogram2DAsCsv(h, this.schema, [this.xAxes[xAxis], this.yAxes[yAxis]]);
            result = result.concat(lines);
            xAxis++;
            if (xAxis == this.xAxes.length) {
                yAxis++;
                xAxis = yAxis + 1;
            }
        }
        const fileName = "correlations.csv";
        saveAs(fileName, result.join("\n"));
    }

    protected dragStart(): void {
        this.dragStartRectangle();
        const origPos = this.getMousePosition([this.selectionOrigin!.x, this.selectionOrigin!.y]);
        if (origPos == null)
            this.dragging = false;
    }

    protected dragMove(): boolean {
        if (!super.dragMove())
            return false;

        const origPos = this.getMousePosition([this.selectionOrigin!.x, this.selectionOrigin!.y]);
        if (origPos === null)
            return false;
        const position = d3mouse(this.surface!.getCanvas().node());
        const currentPos = this.getMousePosition(position);
        if (currentPos == null) {
            this.hideSelectionRectangle();
            return false;
        }
        if (origPos.chartIndex != currentPos.chartIndex) {
            // We only allow selection within the same chart
            this.hideSelectionRectangle();
            return false;
        }
        this.dragMoveRectangle();
        return true;
    }

    public dragEnd(): boolean {
        if (!super.dragEnd())
            return false;
        const position = d3mouse(this.surface!.getCanvas().node());
        this.selectionCompleted(this.selectionOrigin!.x, position[0], this.selectionOrigin!.y, position[1]);
        return true;
    }

    /**
     * Selection has been completed.  The mouse coordinates are within the canvas.
     */
    protected selectionCompleted(xl: number, xr: number, yl: number, yr: number): void {
        const origPos = this.getMousePosition([xl, yl]);
        const currentPos = this.getMousePosition([xr, yr]);
        if (origPos == null || currentPos == null)
            return;
        if (origPos.chartIndex != currentPos.chartIndex)
            return;
        const fx = this.xAxes[currentPos.xAxisIndex].getFilter(origPos.chartX, currentPos.chartX);
        const fy = this.yAxes[currentPos.yAxisIndex].getFilter(origPos.chartY, currentPos.chartY);
        if (fx == null || fy == null)
            return;
        const filter = {
            filters: [fx, fy],
            complement: d3event.sourceEvent.ctrlKey
        };
        const rr = this.createFilterRequest(filter);
        const renderer = new NewTargetReceiver(new PageTitle(this.page.title.format,
            Converters.filterArrayDescription(filter)),
            this.histoArgs.histos.map(h => h.cd),
            this.schema, this.histoArgs.histos.map(_ => 0), this.page, rr, this.dataset, {
                chartKind: "CorrelationHeatmaps", reusePage: false,
            });
        rr.invoke(renderer);
    }

    protected getCombineRenderer(title: PageTitle): (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        const cds = this.histoArgs.histos.map(b => b.cd);
        const zeros = cds.map(_ => 0);
        const schema = this.schema;
        const dataset = this.dataset;
        return function (page: FullPage, operation: ICancellable<RemoteObjectId>) {
            return new NewTargetReceiver(title, cds,
                schema, zeros, page, operation, dataset, {
                    chartKind: "CorrelationHeatmaps", reusePage: false,
                });
        };
    }

    /**
     * Gets the chart where the mouse currently is, or null if the mouse does not overlap a chart.
     * @param position   Array with two mouse coordinates x and y in canvas.
     */
    protected getMousePosition(position: number[]): MousePosition | null {
        const charts = this.histoArgs.histos.length - 1;
        const x = position[0] - this.surface!.leftMargin;
        const y = position[1] - this.surface!.topMargin - this.headerHeight;
        if (x < 0 || x > charts * this.chartSize)
            return null;
        if (y < 0 || y > charts * this.chartSize)
            return null;

        const chartXIndex = Math.floor(x / this.chartSize);
        const chartYIndex = Math.floor(y / this.chartSize);
        const chartX = x - chartXIndex * this.chartSize;
        const chartY = y - chartYIndex * this.chartSize;
        if (chartYIndex > chartXIndex)
            return null;

        let plotIndex = 0;
        for (let i = 0; i < chartYIndex; i++) {
            plotIndex += charts - i;
        }
        plotIndex += chartXIndex - chartYIndex;
        return {
            chartIndex: plotIndex,
            chartX,
            chartY,
            xAxisIndex: chartXIndex + 1,
            yAxisIndex: chartYIndex
        };
    }

    protected onMouseMove(): void {
        const position = d3mouse(this.surface!.getCanvas().node());
        const pos = this.getMousePosition(position);
        if (pos == null) {
            this.pointDescription!.show(false);
            return;
        }
        const plot = this.hps[pos.chartIndex];
        const value = plot.getCount(pos.chartX, pos.chartY);
        const xs = this.xAxes[pos.xAxisIndex].invert(pos.chartX);
        const xname = this.xAxes[pos.xAxisIndex].description!.name;
        const yname = this.yAxes[pos.yAxisIndex].description!.name;
        const ys = this.yAxes[pos.yAxisIndex].invert(pos.chartY);
        this.pointDescription!.show(true);
        const p = d3mouse(this.surface!.getCanvas().node());
        this.pointDescription!.update([xname, yname, xs, ys, makeInterval(value)],
            p[0], p[1]);
    }

    refresh(): void {
        const collector = new DataRangesReceiver(this,
            this.page, null, this.schema, this.histoArgs.histos.map(b => b.bucketCount),
            this.histoArgs.histos.map(b => b.cd), this.page.title, null,{
                chartKind: "CorrelationHeatmaps", reusePage: true
            });
        collector.run(this.ranges);
        collector.finished();
    }

    resize(): void {
        this.updateView(this.data, true);
    }

    protected showTrellis(colName: DisplayName): void { /* not used */ }

    protected legendSelectionCompleted(xl: number, xr: number): void {
        const [min, max] = reorder(this.colorLegend.invert(xl), this.colorLegend.invert(xr));
        const h = this.data.map(g => Heatmap.create(g));
        const bitmap = h.map(g => g.bitmap((c) => min <= c && c <= max));
        const bucketCount = bitmap.reduce((n, b) => n + b.sum(), 0);
        const filter = h.map(g => g.map((c) => min <= c && c <= max ? c : 0));
        const pointCount = filter.reduce((n, b) => n + b.sum(), 0);
        const shiftPressed = d3event.sourceEvent.shiftKey;
        assert(this.summary != null);
        this.summary.set("buckets selected", bucketCount);
        this.summary.set("points selected", pointCount);
        this.summary.display();
        if (shiftPressed) {
            this.colorLegend.emphasizeRange(xl, xr);
        }
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
        // noinspection JSSuspiciousNameCombination
        this.surface = new HtmlPlottingSurface(this.chartDiv!, this.page, {
            height: PlottingSurface.getDefaultCanvasSize(this.page.getWidthInPixels()).width
        });

        this.surfaces = [];
        this.coordinates = [];
        const windows = this.histoArgs.histos.length - 1;
        this.chartSize = Math.round(this.surface.getActualChartSize().width / windows);
        this.headerHeight = Resolution.lineHeight;

        // noinspection JSSuspiciousNameCombination
        const chartHeight = this.chartSize;
        for (let y = 0; y < this.histoArgs.histos.length; y++) {
            for (let x = y + 1; x < this.histoArgs.histos.length; x++) {
                const xCorner = this.surface.leftMargin + (x - 1) * this.chartSize;
                const yCorner = y * chartHeight
                    + this.headerHeight + this.surface.topMargin;
                const surface = this.surface.createChildSurface(xCorner, yCorner, {
                    width: this.chartSize,
                    height: chartHeight,
                    topMargin: 0,
                    leftMargin: 0,
                    bottomMargin: 0,
                    rightMargin: 0 });
                this.surfaces.push(surface);
                this.coordinates.push( {
                    x: xCorner,
                    y: yCorner
                } );
                const hp = new HeatmapPlot(
                    surface, this.colorLegend.getColorMap(),
                    null, 0,
                    false);
                this.hps.push(hp);
            }
        }
        for (let x = 1; x < this.histoArgs.histos.length; x++) {
            const title = this.histoArgs.histos[x].cd.name;
            const canvas = this.surface.getCanvas();
            const xCorner = this.surface.leftMargin + (x - 1) * this.chartSize;
            canvas.append("text")
                .text(truncate(title, 20))
                .attr("class", "trellisTitle")
                .attr("x", xCorner + this.chartSize / 2)
                .attr("y", (this.headerHeight / 2))
                .attr("text-anchor", "middle")
                .attr("dominant-baseline", "middle")
                .append("title")
                .text(title);
        }
        for (let i = 0; i < this.histoArgs.histos.length - 1; i++) {
            const title = this.histoArgs.histos[i].cd.name;
            const canvas = this.surface.getCanvas();
            const x = this.surface.leftMargin / 2;
            const y = i * chartHeight + this.headerHeight + this.surface.topMargin + chartHeight / 2;
            canvas.append("text")
                .text(truncate(title, 20))
                .attr("class", "trellisTitle")
                .attr("x", x)
                .attr("y", y)
                .attr("text-anchor", "middle")
                .attr("dominant-baseline", "middle")
                .attr("transform", () => `rotate(-90, ${x}, ${y})`)
                .append("title")
                .text(title);
        }
    }

    updateView(data: Groups<Groups<number>>[], keepColorMap: boolean) {
        if (data === null)
            return;
        this.data = data;
        this.createNewSurfaces(keepColorMap);
        assert(this.histoArgs.histos.length * (this.histoArgs.histos.length - 1) / 2 === data.length);
        const charts = this.histoArgs.histos.length;
        this.setupMouse();
        assert(this.surface != null);
        this.pointDescription = new TextOverlay(this.surface.getCanvas(),
            this.surface.getActualChartSize(),
            ["X", "Y", "x", "y", "count"], 40);

        for (const a of this.xAxes)
            a.setResolution(this.chartSize, AxisKind.Bottom, PlottingSurface.bottomMargin);
        for (const a of this.yAxes)
            a.setResolution(this.chartSize, AxisKind.Left, PlottingSurface.leftMargin);

        let max = 0;
        let index = 0;
        for (let y = 0; y < charts; y++) {
            for (let x = y + 1; x < charts; x++) {
                this.hps[index].setData({ first: data[index], second: null, third: null },
                    this.xAxes[x],  this.yAxes[y], null,
                    this.schema, 0, this.isPrivate());
                max = Math.max(max, this.hps[index].getMaxCount());
                index++;
            }
        }

        this.colorLegend.setData({first: 1, second: max });
        if (!keepColorMap)
            this.colorLegend.setColorMapKind(ColorMapKind.Grayscale);
        this.colorLegend.draw();
        for (const plot of this.hps) {
            plot.draw();
            plot.border(1);
        }

        for (let i = 0; i < charts - 1; i++) {
            const gx = this.surface
                .getCanvas()
                .append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(
                    ${this.surface.leftMargin + i * this.chartSize}, 
                    ${this.surface.topMargin + this.headerHeight + this.chartSize * (charts - 1)})`);
            this.xAxes[i + 1].axis.draw(gx);
            const gy = this.surface.getCanvas()
                .append("g")
                .attr("class", "y-axis")
                .attr("transform", `translate(
                    ${this.surface.leftMargin},
                    ${this.surface.topMargin + this.headerHeight + i * this.chartSize})`);
            this.yAxes[i].axis.draw(gy);
        }
        this.summary.set("points", this.rowCount);
        this.summary.display();
    }
}

interface MousePosition {
    chartIndex: number;  // chart index in hps array
    chartX: number;  // position within chart
    chartY: number;
    xAxisIndex: number;  // index within the xAxes array of a CorrelationHeatmapView
    yAxisIndex: number;  // index within the yAxes array
}

export class CorrelationHeatmapReceiver extends ReceiverCommon<Groups<Groups<number>>[]> {
    protected view: CorrelationHeatmapView;

    constructor(common: ReceiverCommonArgs, histoArgs: HistogramRequestInfo, ranges: BucketsInfo[],
                operation: RpcRequest<Groups<Groups<number>>[]>) {
        super(common, operation, "correlations")
        this.view = new CorrelationHeatmapView(this.args, histoArgs, ranges, this.page);
    }

    public onNext(value: PartialResult<Groups<Groups<number>>[]>): void {
        super.onNext(value);
        if (value == null)
            return;
        this.view.updateView(value.data, false);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
    }
}
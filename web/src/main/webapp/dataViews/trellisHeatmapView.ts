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
    CombineOperators, Heatmap, Heatmap3D, RemoteObjectId,
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {TableTargetAPI} from "../tableTarget";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {Resolution} from "../ui/ui";
import {AxisData, AxisKind} from "./axisData";
import {TrellisShape} from "./dataRangesCollectors";
import {Receiver} from "../rpc";
import {ICancellable, PartialResult} from "../util";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {
    IViewSerialization,
    TrellisHeatmapSerialization,
    TrellisHistogramSerialization
} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {mouse as d3mouse} from "d3-selection";
import {ChartView} from "./chartView";
import {TextOverlay} from "../ui/textOverlay";

/**
 * A Trellis plot containing multiple heatmaps.
 */
export class TrellisHeatmapView extends ChartView {
    private readonly colorLegend: HeatmapLegendPlot;
    private readonly legendSurface: PlottingSurface;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected groupbyAxisData: AxisData;
    protected hps: HeatmapPlot[];

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                protected shape: TrellisShape,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "TrellisHeatmap");
        this.hps = [];
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");

        this.menu = new TopMenu( [
            { text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                { text: "refresh",
                    action: () => { this.refresh(); },
                    help: "Redraw this view.",
                }, {
                text: "swap axes",
                    action: () => { this.swapAxes(); },
                    help: "Swap the X and Y axes of all plots.",
                }, { text: "table",
                    action: () => { this.showTable(); },
                    help: "Show the data underlying this view in a tabular view.",
                },
            ]) },
        ]);
        this.page.setMenu(this.menu);
        this.legendSurface = new HtmlPlottingSurface(this.topLevel, page);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);
        this.surface = new HtmlPlottingSurface(this.topLevel, this.page);
        this.surface.create();

        for (let y = 0; y < this.shape.yNum; y++) {
            for (let x = 0; x < this.shape.xNum; x++) {
                const surface = this.surface.createChildSurface(
                    PlottingSurface.leftMargin + x * this.shape.size.width,
                    y * this.shape.size.height + PlottingSurface.topMargin);
                surface.setSize(this.shape.size);
                surface.setMargins(0, 0, 0, 0);
                const hp = new HeatmapPlot(surface, this.colorLegend, false);
                hp.clear();
                this.hps.push(hp);
            }
        }
    }

    public static reconstruct(vs: TrellisHeatmapSerialization, page: FullPage): IDataView {
        // TODO
        return null;
    }

    public serialize(): IViewSerialization {
        const ser: TrellisHistogramSerialization = {
            ...super.serialize()
            // TODO
        };
        return ser;
    }

    public setAxes(xAxisData: AxisData,
                   yAxisData: AxisData,
                   groupByAxisData: AxisData): void {
        this.xAxisData = xAxisData;
        this.yAxisData = yAxisData;
        this.groupbyAxisData = groupByAxisData;
    }

    public combine(how: CombineOperators): void {
        // not yet used. TODO: add this menu
    }

    public refresh(): void {
        // TODO
    }

    public swapAxes(): void {
        // TODO
    }

    public showTable(): void {
        // TODO
    }

    public updateView(heatmaps: Heatmap3D, timeInMs: number): void {
        this.page.reportTime(timeInMs);
        this.colorLegend.clear();
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
            plot.setData(heatmap, this.xAxisData, this.yAxisData);
            max = Math.max(max, plot.getMaxCount());
        }

        this.colorLegend.setData(1, max);
        this.colorLegend.draw();
        for (const plot of this.hps) {
            plot.draw();
            plot.border(1);
        }

        this.xAxisData.setResolution(this.shape.size.width, AxisKind.Bottom);
        for (let i = 0; i < this.shape.xNum; i++) {
            this.surface
                .getCanvas()
                .append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(
                    ${PlottingSurface.leftMargin + i * this.shape.size.width}, 
                    ${PlottingSurface.topMargin + this.shape.size.height * this.shape.yNum})`)
                .call(this.xAxisData.axis);
        }

        // This axis is only created when the surface is drawn
        this.yAxisData.setResolution(this.shape.size.height, AxisKind.Left);
        for (let i = 0; i < this.shape.yNum; i++) {
            this.surface.getCanvas()
                .append("g")
                .attr("class", "y-axis")
                .attr("transform", `translate(${PlottingSurface.leftMargin},
                                              ${PlottingSurface.topMargin + 
                                                i * this.shape.size.height})`)
                .call(this.yAxisData.axis);
        }

        this.setupMouse();
        this.pointDescription = new TextOverlay(this.surface.getChart(),
            this.surface.getActualChartSize(),
            [this.xAxisData.description.name,
                this.yAxisData.description.name,
                this.groupbyAxisData.description.name,
                "count"], 40);
    }

    public onMouseMove(): void {
        const position = d3mouse(this.surface.getChart().node());
        let mouseX = position[0];
        let mouseY = position[1];
        const origMouseX = mouseX;
        const origMouseY = mouseY;

        // Find out which plot we are in.
        const xIndex = Math.floor(mouseX / this.shape.size.width);
        const yIndex = Math.floor(mouseY / this.shape.size.height);
        const plotIndex = yIndex * this.shape.xNum + xIndex;
        if (plotIndex < 0 || plotIndex >= this.hps.length) {
            this.pointDescription.show(false);
            return;
        }

        this.pointDescription.show(true);
        const plot = this.hps[plotIndex];
        mouseX -= xIndex * this.shape.size.width;
        mouseY -= yIndex * this.shape.size.height;
        const xs = this.xAxisData.invert(mouseX);
        const ys = this.yAxisData.invert(mouseY);
        const value = plot.getCount(mouseX, mouseY);
        const group = this.groupbyAxisData.bucketDescription(plotIndex);
        this.pointDescription.update([xs, ys, group, value.toString()], origMouseX, origMouseY);
    }

    public dragStart(): void {
        this.dragStartRectangle();
    }

    public dragMove(): boolean {
        return this.dragMoveRectangle();
    }

    public dragEnd(): boolean {
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
        // TODO
    }
}

/**
 * Renders a Trellis plot of heatmaps
 */
export class TrellisHeatmapRenderer extends Receiver<Heatmap3D> {
    protected trellisView: TrellisHeatmapView;

    constructor(page: FullPage,
                remoteTable: TableTargetAPI,
                protected rowCount: number,
                protected schema: SchemaClass,
                protected axes: AxisData[],
                protected samplingRate: number,
                protected shape: TrellisShape,
                operation: ICancellable<Heatmap3D>,
                protected reusePage: boolean) {
        super(
            reusePage ? page : page.dataset.newPage(
                "Heatmaps " + schema.displayName(axes[0].description.name) +
                ", " + schema.displayName(axes[1].description.name) +
                " grouped by " +
                schema.displayName(axes[2].description.name), page),
            operation, "histogram");

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

        this.trellisView.updateView(value.data, this.elapsedMilliseconds());
    }
}

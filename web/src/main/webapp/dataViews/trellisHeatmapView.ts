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
import {BigTableView, TableTargetAPI} from "../tableTarget";
import {FullPage} from "../ui/fullPage";
import {HeatmapLegendPlot} from "../ui/legendPlot";
import {SubMenu, TopMenu} from "../ui/menu";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {Resolution} from "../ui/ui";
import {AxisData} from "./axisData";
import {TrellisShape} from "./dataRangesCollectors";
import {Receiver} from "../rpc";
import {ICancellable, PartialResult} from "../util";
import {HeatmapPlot} from "../ui/heatmapPlot";
import {drag as d3drag} from "d3-drag";

/**
 * A Trellis plot containing multiple heatmaps.
 */
export class TrellisHeatmapView extends BigTableView {
    private readonly colorLegend: HeatmapLegendPlot;
    private readonly legendSurface: PlottingSurface;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected hps: HeatmapPlot[];

    constructor(remoteObjectId: RemoteObjectId,
                rowCount: number,
                schema: SchemaClass,
                protected shape: TrellisShape,
                protected samplingRate: number,
                page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Trellis");
        this.hps = [];
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("chart");

        const menu = new TopMenu( [
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
        this.page.setMenu(menu);
        this.legendSurface = new HtmlPlottingSurface(this.topLevel, page);
        this.legendSurface.setHeight(Resolution.legendSpaceHeight);
        this.colorLegend = new HeatmapLegendPlot(this.legendSurface);

        const table: HTMLTableElement = document.createElement("table");
        const tBody = table.createTBody();
        this.topLevel.appendChild(table);
        for (let y = 0; y < this.shape.yNum; y++) {
            const row = tBody.insertRow();
            for (let x = 0; x < this.shape.xNum; x++) {
                const cell = row.insertCell();
                const surface = new HtmlPlottingSurface(cell, this.page);
                surface.setSize(this.shape.size);
                surface.setMargins(0, 0, 0, 0);
                const hp = new HeatmapPlot(surface, this.colorLegend, false);
                this.hps.push(hp);
            }
        }

        /*
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
         */
    }

    public setAxes(xAxisData: AxisData,
                   yAxisData: AxisData) {
        this.xAxisData = xAxisData;
        this.yAxisData = yAxisData;
    }

    public combine(how: CombineOperators): void {
        // not yet used. TODO: add this menu
    }

    public refresh(): void {
        // TODO
    }

    public swapAxes() {
        // TODO
    }

    public showTable() {
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
            plot.clear();
            max = Math.max(max, plot.getMaxCount());
        }

        this.colorLegend.setData(1, max);
        this.colorLegend.draw();
        for (const plot of this.hps)
            plot.draw();
    }

    private mouseMove() {
        // TODO
    }

    private mouseLeave() {
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
        this.trellisView.setAxes(axes[0], axes[1]);
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

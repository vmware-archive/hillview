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

import {Groups, HistogramRequestInfo, RemoteObjectId} from "../javaBridge";
import {ChartView} from "../modules";
import {FullPage, PageTitle} from "../ui/fullPage";
import {assert, ICancellable, PartialResult, truncate} from "../util";
import {DisplayName} from "../schemaClass";
import {BaseReceiver} from "../modules";
import {RpcRequest} from "../rpc";
import {CommonArgs, ReceiverCommon} from "../ui/receiver";
import {SubMenu, TopMenu} from "../ui/menu";
import {CorrelationHeatmapSerialization, IViewSerialization} from "../datasetView";
import {IDataView} from "../ui/dataview";
import {HtmlPlottingSurface, PlottingSurface} from "../ui/plottingSurface";
import {Point, Resolution} from "../ui/ui";
import {HeatmapLegendPlot} from "../ui/heatmapLegendPlot";
import {HeatmapPlot} from "../ui/heatmapPlot";

export class CorrelationHeatmapView extends ChartView<Groups<Groups<number>>[]> {
    private legendSurface: HtmlPlottingSurface;
    private readonly legendDiv: HTMLDivElement;
    private colorLegend: HeatmapLegendPlot;
    protected hps: HeatmapPlot[];
    protected surfaces: PlottingSurface[];
    /**
     * Coordinates of each surface within the canvas.
     */
    protected coordinates: Point[];

    constructor(args: CommonArgs, protected histoArgs: HistogramRequestInfo[], page: FullPage) {
        super(args.remoteObjectId.remoteObjectId, args.rowCount, args.schema, page, "CorrelationHeatmaps")
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
        this.legendDiv = this.makeToplevelDiv();
    }

    public static reconstruct(ser: CorrelationHeatmapSerialization, page: FullPage): IDataView {
        // TODO
        return null;
    }

    public serialize(): IViewSerialization {
        return null;
    }

    protected export(): void {
        // TODO
    }

    protected getCombineRenderer(title: PageTitle): (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return function (p1: FullPage, p2: ICancellable<RemoteObjectId>) {
            return null;
        };
    }

    protected onMouseMove(): void {
        // TODO
    }

    refresh(): void {
        this.updateView(this.data, true);
    }

    resize(): void {
        // TODO
    }

    protected showTrellis(colName: DisplayName): void {}

    protected createNewSurfaces(keepColorMap: boolean): void {
        if (this.surface != null)
            this.surface.destroy();
        if (this.legendSurface != null)
            this.legendSurface.destroy();
        this.legendSurface = new HtmlPlottingSurface(this.legendDiv, this.page, {
            height: Resolution.legendSpaceHeight });
        if (keepColorMap)
            this.colorLegend.setSurface(this.legendSurface);
        else {
            this.colorLegend = new HeatmapLegendPlot(
                this.legendSurface, (xl, xr) => this.colorLegend.emphasizeRange(xl, xr));
            this.colorLegend.setColorMapChangeEventListener(
                () => this.updateView(this.data, true));
        }
        this.hps = [];
        // noinspection JSSuspiciousNameCombination
        this.surface = new HtmlPlottingSurface(this.topLevel, this.page, {
            height: PlottingSurface.getDefaultCanvasSize(this.page.getWidthInPixels()).width
        });

        this.surfaces = [];
        this.coordinates = [];
        const windows = this.histoArgs.length - 1;
        const chartWidth = Math.round(this.surface.getActualChartSize().width / windows);
        const headerHeight = Resolution.lineHeight;
        // noinspection JSSuspiciousNameCombination
        const chartHeight = chartWidth;
        for (let y = 0; y < this.histoArgs.length; y++) {
            for (let x = y + 1; x < this.histoArgs.length; x++) {
                const xCorner = this.surface.leftMargin + (x - 1) * chartWidth;
                const yCorner = y * chartHeight
                    + headerHeight + this.surface.topMargin;
                const surface = this.surface.createChildSurface(xCorner, yCorner, {
                    width: chartWidth,
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
                const hp = new HeatmapPlot(surface, this.colorLegend, false);
                this.hps.push(hp);
            }
        }
        for (let x = 1; x < this.histoArgs.length; x++) {
            const title = this.histoArgs[x].cd.name;
            const canvas = this.surface.getCanvas();
            const xCorner = this.surface.leftMargin + (x - 1) * chartWidth;
            canvas.append("text")
                .text(truncate(title, 20))
                .attr("class", "trellisTitle")
                .attr("x", xCorner + chartWidth / 2)
                .attr("y", (headerHeight / 2))
                .attr("text-anchor", "middle")
                .attr("dominant-baseline", "middle")
                .append("title")
                .text(title);
        }
        for (let i = 0; i < this.histoArgs.length - 1; i++) {
            const title = this.histoArgs[i].cd.name;
            const canvas = this.surface.getCanvas();
            const x = this.surface.leftMargin / 2;
            const y = i * chartHeight + headerHeight + this.surface.topMargin + chartHeight / 2;
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
        assert(this.histoArgs.length * (this.histoArgs.length - 1) / 2 === data.length);
        const charts = this.histoArgs.length;

        let max = 0;
        let index = 0;
        for (let y = 0; y < charts; y++) {
            for (let x = y + 1; x < charts; x++) {
                this.hps[index].setData({first: data[index], second: null}, null, null,
                    this.schema, 0, this.isPrivate());
                max = Math.max(max, this.hps[index].getMaxCount());
                index++;
            }
        }

        this.colorLegend.setData({first: 1, second: max });
        this.colorLegend.draw();
        for (const plot of this.hps) {
            plot.draw();
            plot.border(1);
        }
    }
}

export class CorrelationHeatmapReceiver extends ReceiverCommon<Groups<Groups<number>>[]> {
    protected view: CorrelationHeatmapView;

    constructor(common: CommonArgs, histoArgs: HistogramRequestInfo[],
                operation: RpcRequest<PartialResult<Groups<Groups<number>>[]>>) {
        super(common, operation, "correlations")
        this.view = new CorrelationHeatmapView(this.args, histoArgs, this.page);
        this.page.setDataView(this.view);
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
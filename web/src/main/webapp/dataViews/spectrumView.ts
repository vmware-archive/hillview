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

import { Renderer } from "../rpc";
import {
    IColumnDescription, Schema, RecordOrder, RangeInfo, Histogram,
    BasicColStats, FilterDescription, HistogramArgs, CombineOperators, RemoteObjectId
} from "../javaBridge";
import {TopMenu, SubMenu} from "../ui/menu";
// noinspection ES6UnusedImports
import {
    Pair, reorder, significantDigits, formatNumber, percent, ICancellable, PartialResult, Seed,
    formatDate, exponentialDistribution
} from "../util";
import {Dialog} from "../ui/dialog";
import {CategoryCache} from "../categoryCache";
import {FullPage} from "../ui/fullPage";
import {Resolution} from "../ui/ui";
import {TextOverlay} from "../ui/textOverlay";
import {AxisData} from "./axisData";
import {HistogramViewBase, BucketDialog} from "./histogramViewBase";
import {Range2DCollector} from "./heatMapView";
import {NextKReceiver, TableView} from "./tableView";
import {RemoteTableObject, RemoteTableObjectView, RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";
import {HistogramPlot} from "../ui/histogramPlot";
import {PlottingSurface} from "../ui/plottingSurface";
import {CDFPlot} from "../ui/CDFPlot";
import {drag as d3drag} from "d3-drag";
import {mouse as d3mouse, event as d3event} from "d3-selection";

/**
 * A HistogramView is responsible for showing a one-dimensional histogram on the screen.
 */
export class SpectrumView extends RemoteTableObjectView {
    protected currentData: {
        histogram: Histogram,
        axisData: AxisData,
        title: string,
    };
    protected menu: TopMenu;
    protected plot: HistogramPlot;
    protected surface: PlottingSurface;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId, page: FullPage) {
        super(remoteObjectId, originalTableId, page);

        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);
        this.chartDiv.style.display = "flex";
        this.chartDiv.style.flexDirection = "column";
        this.surface = new PlottingSurface(this.chartDiv, page);
        this.plot = new HistogramPlot(this.surface);
        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    public updateView(title: string, h: Histogram,
                      axisData: AxisData, elapsedMs: number) : void {
        this.page.reportTime(elapsedMs);
        this.plot.clear();
        if (h == null) {
            this.page.reportError("No data to display");
            return;
        }

        this.currentData = {
            axisData: axisData,
            title: title,
            histogram: h };

        let counts = h.buckets;
        let bucketCount = counts.length;

        this.plot.setHistogram(h, 1, axisData);
        this.plot.draw();
        let canvas = this.surface.getCanvas();

        let summary = "";
        if (h.missingData != 0)
            summary = formatNumber(h.missingData) + " missing, ";
        summary += formatNumber(axisData.stats.presentCount + axisData.stats.missingCount) + " points";
        if (axisData.distinctStrings != null)
            summary += ", " + (axisData.stats.max - axisData.stats.min) + " distinct values";
        summary += ", " + String(bucketCount) + " buckets";
        this.summary.innerHTML = summary;
    }

    public refresh(): void {
        if (this.currentData == null)
            return;
        this.updateView(
            this.currentData.title,
            this.currentData.histogram,
            this.currentData.axisData,
            0);
    }

    public mouseMove(): void {
        let position = d3mouse(this.surface.getChart().node());
        let mouseX = position[0];
        let mouseY = position[1];

        let xs = "";
        if (this.plot.xScale != null) {
            xs = HistogramViewBase.invert(
                position[0], this.plot.xScale, this.currentData.axisData.description.kind, this.currentData.axisData.distinctStrings)
        }
        let y = Math.round(this.plot.yScale.invert(position[1]));
        let ys = significantDigits(y);
        let mouseLabel = [xs, ys];

        //this.pointDescription.update(mouseLabel, mouseX, mouseY);
    }
}

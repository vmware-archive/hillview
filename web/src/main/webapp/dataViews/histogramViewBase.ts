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

import {mouse as d3mouse} from "d3-selection";
import {
    RemoteObjectId
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {CDFPlot} from "../ui/CDFPlot";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {PlottingSurface} from "../ui/plottingSurface";
import {D3SvgElement, Resolution, SpecialChars, ViewKind} from "../ui/ui";
import {significantDigits} from "../util";
import {ChartView} from "./chartView";

/**
 * This is a base class that contains code common to various histogram renderings.
 */
export abstract class HistogramViewBase extends ChartView {
    protected summary: HTMLElement;
    protected cdfDot: D3SvgElement;
    protected cdfPlot: CDFPlot;
    protected chartDiv: HTMLDivElement;

    protected constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage, viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";

        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);
        this.chartDiv.style.display = "flex";
        this.chartDiv.style.flexDirection = "column";

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code === "Escape")
            this.cancelDrag();
    }

    protected abstract showTable(): void;
    public abstract resize(): void;

    /**
     * Dragging started in the canvas.
     */
    public dragStart(): void {
        super.dragStart();
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    /**
     * The mouse moved in the canvas.
     */
    protected dragMove(): boolean {
        if (!super.dragMove())
            return false;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.surface.getChartHeight();

        if (width < 0) {
            ox = x;
            width = -width;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", this.surface.topMargin)
            .attr("width", width)
            .attr("height", height);
        return true;
    }

    /**
     * Compute the string used to display the height of a box in a histogram
     * @param  barSize       Bar size as reported by histogram.
     * @param  samplingRate  Sampling rate that was used to compute the box height.
     * @param  totalPop      Total population which was sampled to get this box.
     */
    public static boxHeight(barSize: number, samplingRate: number, totalPop: number): string {
        if (samplingRate >= 1) {
            if (barSize === 0)
                return "";
            return significantDigits(barSize);
        }
        const muS = barSize / totalPop;
        const dev = 2.38 * Math.sqrt(muS * (1 - muS) * totalPop / samplingRate);
        const min = Math.max(barSize - dev, 0);
        const max = barSize + dev;
        const minString = significantDigits(min);
        const maxString = significantDigits(max);
        if (minString === maxString && dev !== 0)
            return minString;
        return SpecialChars.approx + significantDigits(barSize);
    }

    /**
     * The number of "buckets" needed for displaying a heatmap.
     */
    public static maxHeatmapBuckets(page: FullPage): [number, number] {
        const size = PlottingSurface.getDefaultChartSize(page.getWidthInPixels());
        return [Math.floor(size.width / Resolution.minDotSize),
            Math.floor(size.height / Resolution.minDotSize)];
    }

    /**
     * The max number of buckets to use when requesting the range of
     * data for displaying a 2D histogram.
     */
    public static maxHistogram2DBuckets(page: FullPage): [number, number] {
        // On the horizontal axis we get the maximum resolution, which we will use for
        // deriving the CDF curve.  On the vertical axis we use a smaller number.
        return [page.getWidthInPixels(), Resolution.maxBucketCount];
    }

    /**
     * The max number of buckets to use when requesting the range of
     * data for displaying a 2D Trellis plot.
     */
    public static maxTrellis2DBuckets(page: FullPage): [number, number] {
        const width = page.getWidthInPixels();
        const maxWindows = Math.floor(width / Resolution.minTrellisWindowSize) *
            Math.floor(PlottingSurface.getDefaultCanvasSize(width).height / Resolution.minTrellisWindowSize);
        return [page.getWidthInPixels(), maxWindows];
    }

    /**
     * The max number of buckets to use when requesting the range of
     * data for displaying a 3D Trellism plot.
     */
    public static maxTrellis3DBuckets(page: FullPage): [number, number, number] {
        const width = page.getWidthInPixels();
        const maxWindows = Math.floor(width / Resolution.minTrellisWindowSize) *
            Math.floor(PlottingSurface.getDefaultCanvasSize(width).height / Resolution.minTrellisWindowSize);
        return [page.getWidthInPixels(), Resolution.maxBucketCount, maxWindows];
    }
}

/**
 * A dialog that queries the user about the number of buckets to use
 * in a histogram rendering.
 */
export class BucketDialog extends Dialog {
    constructor() {
        super("Set buckets", "Change the number of buckets (bars) used to display the histogram.");
        this.addTextField("n_buckets", "Number of buckets:", FieldKind.Integer, null,
            "The number of buckets to use; must be between 1 and " + Resolution.maxBucketCount);
        this.setCacheTitle("BucketDialog");
    }

    public getBucketCount(): number {
        return this.getFieldValueAsInt("n_buckets");
    }
}

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
    DataRange, HistogramArgs,
    IColumnDescription,
    kindIsString,
    RemoteObjectId
} from "../javaBridge";
import {SchemaClass} from "../schemaClass";
import {CDFPlot} from "../ui/CDFPlot";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {PlottingSurface} from "../ui/plottingSurface";
import {TextOverlay} from "../ui/textOverlay";
import {D3SvgElement, Point, Resolution, Size, SpecialChars, ViewKind} from "../ui/ui";
import {periodicSamples, Seed, significantDigits} from "../util";
import {BigTableView} from "../tableTarget";

/**
 * This is a base class that contains code common to various histogram renderings.
 */
export abstract class HistogramViewBase extends BigTableView {
    protected dragging: boolean;
    protected svg: D3SvgElement;
    /**
     * Coordinates are within the canvas, not within the chart.
     */
    protected selectionOrigin: Point;
    /**
     * The coordinates of the selectionRectangle are relative to the canvas.
     */
    protected selectionRectangle: D3SvgElement;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;
    protected cdfDot: D3SvgElement;
    protected moved: boolean;  // to detect trivial empty drags
    protected pointDescription: TextOverlay;

    protected cdfPlot: CDFPlot;
    protected surface: PlottingSurface;

    protected constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage, viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = (e) => this.keyDown(e);
        this.dragging = false;
        this.moved = false;

        this.topLevel.tabIndex = 1;

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

    protected cancelDrag() {
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    protected abstract showTable(): void;
    public abstract refresh(): void;

    public mouseEnter(): void {
        if (this.pointDescription != null)
            this.pointDescription.show(true);
    }

    public mouseLeave(): void {
        if (this.pointDescription != null)
            this.pointDescription.show(false);
    }

    /**
     * Dragging started in the canvas.
     */
    public dragStart(): void {
        this.dragging = true;
        this.moved = false;
        const position = d3mouse(this.surface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    /**
     * The mouse moved in the canvas.
     */
    public dragMove(): void {
        if (!this.dragging)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.surface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.surface.getActualChartHeight();

        if (width < 0) {
            ox = x;
            width = -width;
        }

        this.selectionRectangle
            .attr("x", ox)
            .attr("y", this.surface.topMargin)
            .attr("width", width)
            .attr("height", height);
    }

    public dragEnd(): void {
        if (!this.dragging || !this.moved)
            return;
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);
    }

    public static computeHistogramArgs(
        cd: IColumnDescription,
        range: DataRange,
        bucketCount: number,  // if 0 the size is chosen based on the screen size
        exact: boolean,
        chartSize: Size): HistogramArgs {
        if (kindIsString(cd.kind)) {
            const cdfBucketCount = range.leftBoundaries.length;
            let samplingRate = HistogramViewBase.samplingRate(
                cdfBucketCount, range.presentCount, chartSize);
            if (exact)
                samplingRate = 1.0;
            let bounds = range.leftBoundaries;
            if (bucketCount !== 0)
                bounds = periodicSamples(range.leftBoundaries, bucketCount);
            const args: HistogramArgs = {
                cd: cd,
                seed: Seed.instance.getSampled(samplingRate),
                samplingRate: samplingRate,
                leftBoundaries: bounds,
                bucketCount: bounds.length
            };
            return args;
        } else {
            let cdfCount = Math.floor(chartSize.width);
            if (bucketCount !== 0)
                cdfCount = bucketCount;

            let adjust = 0;
            if (cd.kind === "Integer") {
                if (cdfCount > range.max - range.min)
                    cdfCount = range.max - range.min + 1;
                adjust = .5;
            }

            let samplingRate = 1.0;
            if (!exact)
                samplingRate = HistogramViewBase.samplingRate(
                    cdfCount, range.presentCount, chartSize);
            const args: HistogramArgs = {
                cd: cd,
                min: range.min - adjust,
                max: range.max + adjust,
                samplingRate: samplingRate,
                seed: Seed.instance.getSampled(samplingRate),
                bucketCount: cdfCount
            };
            return args;
        }
    }

    // noinspection JSUnusedLocalSymbols
    public static samplingRate(bucketCount: number, rowCount: number, chartSize: Size): number {
        const constant = 4;  // This models the confidence we want from the sampling
        const height = chartSize.height;
        const sampleCount = constant * height * height;
        const sampleRate = sampleCount / rowCount;
        return Math.min(sampleRate, 1);
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

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
import {DistinctStrings} from "../distinctStrings";
import {
    ColumnHistogramBoundaries,
    ContentsKind,
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
import {Point, Resolution, SpecialChars, ViewKind} from "../ui/ui";
import {Converters, formatDate, formatNumber, Seed, significantDigits} from "../util";
import {AnyScale, AxisData} from "./axisData";
import {BigTableView} from "../tableTarget";

/**
 * This is a base class that contains code common to various histogram renderings.
 */
export abstract class HistogramViewBase extends BigTableView {
    protected dragging: boolean;
    protected svg: any;
    /**
     * Coordinates are within the canvas, not within the chart.
     */
    protected selectionOrigin: Point;
    /**
     * The coordinates of the selectionRectangle are relative to the canvas.
     */
    protected selectionRectangle: any;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;
    protected cdfDot: any;
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

    public static computeAxis(
        cd: IColumnDescription,
        range: DataRange,
        bucketCount: number): AxisData {

        if (kindIsString(cd.kind)) {
            const cdfBucketCount = range.boundaries.length;
            const distinctStrings = new DistinctStrings(range.boundaries, cd.name);
            const useRange: DataRange = {
                min: -0.5,
                max: cdfBucketCount - .5,
                presentCount: range.presentCount
            };
            return new AxisData(cd, useRange, distinctStrings, cdfBucketCount);
        } else {
            return new AxisData(cd, range, null, bucketCount);
        }
    }

    public static computeHistogramArgs(
        cd: IColumnDescription,
        range: DataRange,
        bucketCount: number,  // ignored for string histograms;
                              // if 0 the size is chosen based on the screen size
        exact: boolean,
        page: FullPage): HistogramArgs {
        if (kindIsString(cd.kind)) {
            const cdfBucketCount = range.boundaries.length;
            let samplingRate = HistogramViewBase.samplingRate(
                cdfBucketCount, range.presentCount, page);
            if (exact)
                samplingRate = 1.0;
            const args: HistogramArgs = {
                cd: cd,
                seed: Seed.instance.getSampled(samplingRate),
                samplingRate: samplingRate,
                boundaries: range.boundaries
            };
            return args;
        } else {
            const size = PlottingSurface.getDefaultChartSize(page);
            let cdfCount = Math.floor(size.width);
            if (bucketCount !== 0)
                cdfCount = bucketCount;

            let samplingRate = 1.0;
            if (!exact)
                samplingRate = HistogramViewBase.samplingRate(cdfCount, range.presentCount, page);

            const args: HistogramArgs = {
                cd: cd,
                min: range.min,
                max: range.max,
                samplingRate: samplingRate,
                seed: Seed.instance.getSampled(samplingRate),
                cdfBucketCount: cdfCount
            };
            return args;
        }
    }

    // noinspection JSUnusedLocalSymbols
    public static samplingRate(bucketCount: number, rowCount: number, page: FullPage): number {
        const constant = 4;  // This models the confidence we want from the sampling
        const height = PlottingSurface.getDefaultChartSize(page).height;
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

    // Adjust the statistics for integral and categorical data pretending
    // that we are centering the values.
    public static adjustStats(kind: ContentsKind, stats: DataRange): void {
         if (kind === "Integer" || kind === "Category") {
             stats.min -= .5;
             stats.max += .5;
         }
    }

    public static getRange(stats: DataRange, cd: IColumnDescription,
                           allStrings: DistinctStrings,
                           bucketCount: number): ColumnHistogramBoundaries {
        const boundaries = (allStrings != null && allStrings.uniqueStrings != null) ?
            allStrings.categoriesInRange(stats.min, stats.max, bucketCount) : null;
        return {
            columnName: cd.name,
            min: stats.min,
            max: stats.max,
            bucketBoundaries: boundaries,
            onStrings: kindIsString(cd.kind)
        };
    }

    /**
     * The number of "buckets" needed for displaying a heatmap.
     */
    public static heatmapSize(page: FullPage): [number, number] {
        const size = PlottingSurface.getDefaultChartSize(page);
        return [Math.floor(size.width / Resolution.minDotSize),
            Math.floor(size.height / Resolution.minDotSize)];
    }

    /**
     * The number of buckets to use when requesting the range of
     * data for displaying a 2D histogram.
     */
    public static histogram2DSize(page: FullPage): [number, number] {
        // On the horizontal axis we get the maximum resolution, which we will use for
        // deriving the CDF curve.  On the vertical axis we use a max smaller number.
        return [page.getWidthInPixels(), Resolution.maxBucketCount];
    }

    /**
     * Computes the number of buckets for a histogram axis.
     * @param range        Data range for that axis.
     * @param page         Page displaying the data.
     * @param columnKind   Kind of data displayed.
     * @param heatmap      True if we want to display a heatmap.
     * @param bottom       True if we are considering the X axis; false for the Y axis.
     */
    public static bucketCount(range: DataRange, page: FullPage,
                              columnKind: ContentsKind,
                              heatmap: boolean, bottom: boolean): number {
        const size = PlottingSurface.getDefaultChartSize(page);
        const length = Math.floor(bottom ? size.width : size.height);
        let maxBucketCount = Resolution.maxBucketCount;
        let minBarWidth = Resolution.minBarWidth;
        if (heatmap) {
            maxBucketCount = length;
            minBarWidth = Resolution.minDotSize;
        }

        let bucketCount = maxBucketCount;
        if (length / minBarWidth < bucketCount)
            bucketCount = Math.floor(length / minBarWidth);

        if (columnKind === "Integer")
            bucketCount = Math.min(
                bucketCount,
                (range.max - range.min) / Math.ceil( (range.max - range.min) / maxBucketCount));

        return Math.floor(bucketCount);
    }

    /**
     * Given a mouse coordinate on a specified d3 scale, returns the corresponding
     * Real-world value.  Returns the result as a string.
     * @param v          Mouse coordinate.
     * @param scale      Axis scale.
     * @param kind       Kind of data on scale.
     * @param allStrings When the axis has string data this is the list of all strings.
     * TODO: this probably should belong to the AxisData class.
     */
    public static invert(v: number, scale: AnyScale, kind: ContentsKind,
                         allStrings: DistinctStrings): string {
        const inv = scale.invert(v);
        let result: string;
        if (kind === "Integer")
            result = formatNumber(Math.round(inv as number));
        if (kindIsString(kind))
            result = allStrings.get(inv as number, true);
        else if (kind === "Integer" || kind === "Double")
            result = formatNumber(inv as number);
        else if (kind === "Date")
            result = formatDate(inv as Date);
        else
            result = inv.toString();
        return result;
    }

    public static invertToNumber(v: number, scale: AnyScale, kind: ContentsKind): number {
        // TODO: move this to the AxisData class.
        const inv = scale.invert(v);
        let result: number = 0;
        if (kind === "Integer" || kindIsString(kind)) {
            result = Math.round(inv as number);
        } else if (kind === "Double") {
            result = inv as number;
        } else if (kind === "Date") {
            result = Converters.doubleFromDate(inv as Date);
        }
        return result;
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

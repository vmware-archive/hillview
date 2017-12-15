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

import {d3} from "../ui/d3-modules";
import {Dialog, FieldKind} from "../ui/dialog";
import {ContentsKind, Schema, BasicColStats, ColumnDescription, ColumnAndRange, RemoteObjectId} from "../javaBridge";
import {Converters, formatDate, significantDigits} from "../util";
import {Point, Resolution, SpecialChars} from "../ui/ui";
import {FullPage} from "../ui/fullPage";
import {TextOverlay} from "../ui/textOverlay";
import {AnyScale} from "./axisData";
import {RemoteTableObjectView} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {CDFPlot} from "../ui/CDFPlot";
import {PlottingSurface} from "../ui/plottingSurface";

/**
 * This is a base class that contains code common to various histogram renderings.
 */
export abstract class HistogramViewBase extends RemoteTableObjectView {
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

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId,
                protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, originalTableId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = e => this.keyDown(e);
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
        if (ev.code == "Escape")
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
        let position = d3.mouse(this.surface.getCanvas().node());
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
        let position = d3.mouse(this.surface.getCanvas().node());
        let x = position[0];
        let width = x - ox;
        let height = this.surface.getActualChartHeight();

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

    // noinspection JSUnusedLocalSymbols
    public static samplingRate(bucketCount: number, rowCount: number, page: FullPage): number {
        let constant = 4;  // This models the confidence we want from the sampling
        let height = PlottingSurface.getDefaultChartSize(page).height;
        let sampleCount = constant * height * height;
        let sampleRate = sampleCount / rowCount;
        return Math.min(sampleRate, 1);
    }

    /**
     * Compute the string used to display the height of a box in a histogram
     * @param  barSize         Bar size as reported by histogram.
     * @param  samplingRate  Sampling rate that was used to compute the box height.
     * @param  totalPop      Total population which was sampled to get this box.
     */
    public static boxHeight(barSize: number, samplingRate: number, totalPop: number): string {
        if (samplingRate >= 1) {
            if (barSize == 0)
                return "";
            return significantDigits(barSize);
        }
        let muS = barSize / totalPop;
        let dev = 2.38 * Math.sqrt(muS * (1 - muS) * totalPop / samplingRate);
        let min = Math.max(barSize - dev, 0);
        let max = barSize + dev;
        let minString = significantDigits(min);
        let maxString = significantDigits(max);
        if (minString == maxString)
            return minString;
        return SpecialChars.approx + significantDigits(barSize);
    }

    // Adjust the statistics for integral and categorical data pretending
    // that we are centering the values.
    public static adjustStats(kind: ContentsKind, stats: BasicColStats): void {
         if (kind == "Integer" || kind == "Category") {
             stats.min -= .5;
             stats.max += .5;
         }
    }

    public static getRange(stats: BasicColStats, cd: ColumnDescription,
                           allStrings: DistinctStrings,
                           bucketCount: number): ColumnAndRange {
        let boundaries = allStrings != null ?
            allStrings.categoriesInRange(stats.min, stats.max, bucketCount) : null;
        return {
            columnName: cd.name,
            min: stats.min,
            max: stats.max,
            bucketBoundaries: boundaries
        };
    }

    public static bucketCount(stats: BasicColStats, page: FullPage, columnKind: ContentsKind,
                              heatMap: boolean, bottom: boolean): number {
        let size = PlottingSurface.getDefaultChartSize(page);
        let length = Math.floor(bottom ? size.width : size.height);
        let maxBucketCount = Resolution.maxBucketCount;
        let minBarWidth = Resolution.minBarWidth;
        if (heatMap) {
            maxBucketCount = length;
            minBarWidth = Resolution.minDotSize;
        }

        let bucketCount = maxBucketCount;
        if (length / minBarWidth < bucketCount)
            bucketCount = Math.floor(length / minBarWidth);

        if (columnKind == "Integer" ||
            columnKind == "Category")
            bucketCount = Math.min(bucketCount, stats.max - stats.min);

        return Math.floor(bucketCount);
    }

    static invert(v: number, scale: AnyScale, kind: ContentsKind,
                  allStrings: DistinctStrings): string {
        let inv = scale.invert(v);
        if (kind == "Integer")
            inv = Math.round(<number>inv);
        let result = String(inv);
        if (kind == "Category")
            result = allStrings.get(<number>inv);
        else if (kind == "Integer" || kind == "Double")
            result = significantDigits(<number>inv);
        else if (kind == "Date")
            result = formatDate(<Date>inv);
        return result;
    }

    public static invertToNumber(v: number, scale: AnyScale, kind: ContentsKind): number {
        let inv = scale.invert(v);
        let result: number = 0;
        if (kind == "Integer" || kind == "Category") {
            result = Math.round(<number>inv);
        } else if (kind == "Double") {
            result = <number>inv;
        } else if (kind == "Date") {
            result = Converters.doubleFromDate(<Date>inv);
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

    getBucketCount(): number {
        return this.getFieldValueAsInt("n_buckets");
    }
}

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
    FullPage, Point, Size, KeyCodes, Resolution, significantDigits, SpecialChars
} from "./ui";
import {Dialog} from "./dialog";
import d3 = require('d3');
import {
    ContentsKind, Schema, RemoteTableObjectView, BasicColStats, DistinctStrings,
    ColumnDescription, ColumnAndRange
} from "./tableData";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {Converters} from "./util";

export type AnyScale = ScaleLinear<number, number> | ScaleTime<number, number>;

export interface ScaleAndAxis {
    scale: AnyScale,
    axis: any,  // a d3 axis, but typing does not work well
}

export abstract class HistogramViewBase extends RemoteTableObjectView {
    protected dragging: boolean;
    protected svg: any;
    protected selectionOrigin: Point;
    protected selectionRectangle: d3.Selection<BaseType, any, BaseType, BaseType>;
    protected xLabel: HTMLElement;
    protected yLabel: HTMLElement;
    protected cdfLabel: HTMLElement;
    protected chartDiv: HTMLElement;
    protected summary: HTMLElement;
    protected xScale: AnyScale;
    protected yScale: ScaleLinear<number, number>;
    protected chartSize: Size;
    protected chart: any;  // these are in fact a d3.Selection<>, but I can't make them typecheck
    protected canvas: any;
    protected xDot: any;
    protected yDot: any;
    protected cdfDot: any;
    protected moved: boolean;  // to detect trivial empty drags

    constructor(remoteObjectId: string, protected tableSchema: Schema, page: FullPage) {
        super(remoteObjectId, page);
        this.topLevel = document.createElement("div");
        this.topLevel.className = "chart";
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.dragging = false;
        this.moved = false;

        this.topLevel.tabIndex = 1;

        this.chartDiv = document.createElement("div");
        this.topLevel.appendChild(this.chartDiv);

        this.summary = document.createElement("div");
        this.topLevel.appendChild(this.summary);

        let position = document.createElement("table");
        this.topLevel.appendChild(position);
        position.className = "noBorder";
        let body = position.createTBody();
        let row = body.insertRow();
        row.className = "noBorder";

        let infoWidth = "150px";
        let labelCell = row.insertCell(0);
        labelCell.width = infoWidth;
        this.xLabel = document.createElement("div");
        this.xLabel.style.textAlign = "left";
        labelCell.appendChild(this.xLabel);
        labelCell.className = "noBorder";

        labelCell = row.insertCell(1);
        labelCell.width = infoWidth;
        this.yLabel = document.createElement("div");
        this.yLabel.style.textAlign = "left";
        labelCell.appendChild(this.yLabel);
        labelCell.className = "noBorder";

        labelCell = row.insertCell(2);
        labelCell.width = infoWidth;
        this.cdfLabel = document.createElement("div");
        this.cdfLabel.style.textAlign = "left";
        labelCell.appendChild(this.cdfLabel);
        labelCell.className = "noBorder";
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.keyCode == KeyCodes.escape)
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
    protected abstract onMouseMove(): void;
    // xl and xr are coordinates of the mouse position within the chart
    protected abstract selectionCompleted(xl: number, xr: number): void;

    protected dragStart(): void {
        this.dragging = true;
        this.moved = false;
        let position = d3.mouse(this.chart.node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    protected dragMove(): void {
        this.onMouseMove();
        if (!this.dragging)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        let position = d3.mouse(this.chart.node());
        let x = position[0];
        let width = x - ox;
        let height = this.chartSize.height;

        if (width < 0) {
            ox = x;
            width = -width;
        }

        this.selectionRectangle
            .attr("x", ox + Resolution.leftMargin)
            .attr("y", Resolution.topMargin)
            .attr("width", width)
            .attr("height", height);
    }

    protected dragEnd(): void {
        if (!this.dragging || !this.moved)
            return;
        this.dragging = false;
        this.selectionRectangle
            .attr("width", 0)
            .attr("height", 0);

        let position = d3.mouse(this.chart.node());
        let x = position[0];
        this.selectionCompleted(this.selectionOrigin.x, x);
    }

    public static samplingRate(bucketCount: number, pointCount: number, page: FullPage): number {
        let height = Resolution.getChartSize(page).height;
        return Math.min(bucketCount * height * Math.log(bucketCount + 2) * 5 / pointCount, 1);
    }

    /**
     * Compute the string used to display the height of a box in a histogram
     * @param  count  Box size as reported by histogram
     * @param  exact  If true the box size is exact, otherwise it is approximate
     * @param  pixelSize  The size of a box that is 1 pixel high.
     */
    protected static boxHeight(count: number, exact: boolean, pixelSize: number): string {
        if (exact) {
            if (count == 0)
                return "";
            return significantDigits(count);
        }
        let min = Math.max(count - pixelSize, 0);
        let max = count + pixelSize;
        let minString = significantDigits(min);
        let maxString = significantDigits(max);
        if (minString == maxString)
            return minString;
        return SpecialChars.approx + significantDigits(count);
    }

    // Adjust the statistics for integral and categorical data
    public static adjustStats(kind: ContentsKind, stats: BasicColStats): void {
         if (kind == "Integer" || kind == "Category") {
             // If we don't do this the bucket boundaries when drawing don't
             // correspond with the bucket boundaries when computing the histogram and
             // the result is bad.
             stats.min -= .5;
             stats.max += .5;
         }
    }

    public static getRange(stats: BasicColStats, page: FullPage,
                           cd: ColumnDescription, allStrings: DistinctStrings,
                           cdfBucketCount: number, exact: boolean, heatMap: boolean, bottom: boolean
    ): ColumnAndRange {
        let bucketCount = HistogramViewBase.bucketCount(stats, page, cd.kind, heatMap, bottom);
        if (cdfBucketCount == 0)
            cdfBucketCount = bucketCount;
        let boundaries = allStrings != null ?
            allStrings.categoriesInRange(stats.min, stats.max, cdfBucketCount) : null;

        let samplingRate = 1.0;
        if (!exact)
            samplingRate = HistogramViewBase.samplingRate(bucketCount, stats.presentCount, page);

        return {
            columnName: cd.name,
            min: stats.min,
            max: stats.max,
            samplingRate: samplingRate,
            bucketCount: bucketCount,
            cdfBucketCount: cdfBucketCount,
            bucketBoundaries: boundaries
        };
    }

    public static bucketCount(stats: BasicColStats, page: FullPage, columnKind: ContentsKind,
                              heatMap: boolean, bottom: boolean): number {
        let size = Resolution.getChartSize(page);
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

    public static createScaleAndAxis(
        kind: ContentsKind, bucketCount: number, width: number,
        min: number, max: number, strings: DistinctStrings,
        bottom: boolean): ScaleAndAxis {
        let axis = null;

        let axisCreator = bottom ? d3.axisBottom : d3.axisLeft;
        // on vertical axis the direction is swapped
        let domain = bottom ? [min, max] : [max, min];

        let scale: AnyScale = null;
        if (kind == "Integer" || kind == "Double") {
            scale = d3.scaleLinear()
                .domain(domain)
                .range([0, width]);
            axis = axisCreator(scale);
        } else if (kind == "Category") {
            let ticks: number[] = [];
            let labels: string[] = [];
            let tickCount = max - min;
            // TODO: if the tick count is too large it must be reduced
            let minLabelWidth = 40;  // pixels
            let maxLabelCount = width / minLabelWidth;
            let labelPeriod = Math.ceil(tickCount / maxLabelCount);
            let tickWidth = width / tickCount;

            for (let i = 0; i < tickCount; i++) {
                ticks.push((i + .5) * tickWidth);
                let label = "";
                if (i % labelPeriod == 0)
                    label = strings.get(min + .5 + i);
                labels.push(label);
            }
            if (!bottom)
                labels.reverse();

            // We manually control the ticks.
            let manual = d3.scaleLinear()
                .domain([0, width])
                .range([0, width]);
            scale = d3.scaleLinear()
                .domain(domain)
                .range([0, width]);
            axis = axisCreator(manual)
                .tickValues(ticks)
                .tickFormat((d, i) => labels[i]);
        } else if (kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(domain[0]);
            let maxDate: Date = Converters.dateFromDouble(domain[1]);
            scale = d3
                .scaleTime()
                .domain([minDate, maxDate])
                .range([0, width]);
            axis = axisCreator(scale);
        }

        return { scale: scale, axis: axis };
    }
}

export class BucketDialog extends Dialog {
    constructor() {
        super("Set buckets");
        this.addTextField("n_buckets", "Number of buckets:", "Integer");
    }

    getBucketCount(): number {
        return this.getFieldValueAsInt("n_buckets");
    }
}

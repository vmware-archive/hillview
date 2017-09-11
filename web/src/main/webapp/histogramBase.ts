/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {
    FullPage, Point, Size, KeyCodes, Resolution
} from "./ui";
import {Dialog} from "./dialog";
import d3 = require('d3');
import {ContentsKind, Schema, RemoteTableObjectView, BasicColStats, DistinctStrings} from "./tableData";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {Converters, isInteger} from "./util";

export type AnyScale = ScaleLinear<number, number> | ScaleTime<number, number>;

export interface ScaleAndAxis {
    scale: AnyScale,
    axis: any,  // a d3 axis, but typing does not work well
    adjustment: number  // adjustment used for categorical and integral data
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
    protected adjustment: number = 0;
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

    public static samplingRate(bucketCount: number, pointCount: number): number {
        return 1.0;
        // TODO: if we sample we lose outliers; we should use the sampling to
        // compute a fast result first.
        // return Math.min(bucketCount * HistogramViewBase.chartHeight * Math.log(bucketCount + 2) * 5 / pointCount, 1);
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
            columnKind == "Category") {
            if (!isInteger(stats.min) || !isInteger(stats.max))
                throw "Expected integer values";
            bucketCount = Math.min(bucketCount, stats.max - stats.min + 1);
        }
        return bucketCount;
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

    // When plotting integer values we increase the data range by .5 on the left and right.
    // The adjustment is the number of pixels on screen that we "waste".
    // I.e., the cdf plot will start adjustment/2 pixels from the chart left margin
    // and will end adjustment/2 pixels from the right margin.
    public static createScaleAndAxis(
        kind: ContentsKind, bucketCount: number, width: number,
        min: number, max: number, strings: DistinctStrings,
        adjustIntegral: boolean,  // if true we perform adjustment for integral values
        bottom: boolean): ScaleAndAxis {
        let minRange = min;
        let maxRange = max;
        let adjustment = 0;
        let axis = null;

        let scaleCreator = bottom ? d3.axisBottom : d3.axisLeft;

        if (adjustIntegral && (kind == "Integer" || kind == "Category" || min >= max)) {
            minRange -= .5;
            maxRange += .5;
            adjustment = width / (maxRange - minRange);
        }

        // on vertical axis the direction is swapped
        let domain = bottom ? [minRange, maxRange] : [maxRange, minRange];

        let scale: AnyScale = null;
        let ordinalScale = null;
        if (kind == "Integer" || kind == "Double") {
            scale = d3.scaleLinear()
                .domain(domain)
                .range([0, width]);
            axis = scaleCreator(scale);
        } else if (kind == "Category") {
            let ticks: number[] = [];
            let labels: string[] = [];
            for (let i = 0; i < bucketCount; i++) {
                let index = i * (maxRange - minRange) / bucketCount;
                index = Math.round(index);
                ticks.push(adjustment / 2 + index * width / (maxRange - minRange));
                labels.push(strings.get(min + index));
            }
            if (!bottom)
                labels.reverse();

            ordinalScale = d3.scaleOrdinal()
                .domain(labels)
                .range(ticks);
            scale = d3.scaleLinear()
                .domain(domain)
                .range([0, width]);
            axis = scaleCreator(ordinalScale);
        } else if (kind == "Date") {
            let minDate: Date = Converters.dateFromDouble(domain[0]);
            let maxDate: Date = Converters.dateFromDouble(domain[1]);
            scale = d3
                .scaleTime()
                .domain([minDate, maxDate])
                .range([0, width]);
            axis = scaleCreator(scale);
        }
        // force a tick on x axis for degenerate scales
        if (min >= max && axis != null)
            axis.ticks(1);

        return { scale: scale, axis: axis, adjustment: adjustment };
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

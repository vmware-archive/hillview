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
import {RemoteObjectView} from "./rpc";
import {ContentsKind, Schema} from "./table";
import {BaseType} from "d3-selection";
import {ScaleLinear, ScaleTime} from "d3-scale";
import {Converters} from "./util";

// same as Java class
export interface Histogram {
    buckets: number[]
    missingData: number;
    outOfRange: number;
}

// same as Java class
export interface BasicColStats {
    momentCount: number;
    min: number;
    max: number;
    minObject: any;
    maxObject: any;
    moments: Array<number>;
    presentCount: number;
    missingCount: number;
}

// Same as Java class
export interface ColumnAndRange {
    min: number;
    max: number;
    samplingRate: number;
    columnName: string;
    bucketCount: number;
    cdfBucketCount: number;
    bucketBoundaries: string[];
}

export interface FilterDescription {
    min: number;
    max: number;
    columnName: string;
    complement: boolean;
    bucketBoundaries: string[];
}

export type AnyScale = ScaleLinear<number, number> | ScaleTime<number, number>;

export abstract class HistogramViewBase extends RemoteObjectView {
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
    // When plotting integer values we increase the data range by .5 on the left and right.
    // The adjustment is the number of pixels on screen that we "waste".
    // I.e., the cdf plot will start adjustment/2 pixels from the chart left margin
    // and will end adjustment/2 pixels from the right margin.
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

    public static bucketCount(stats: BasicColStats, page: FullPage, columnKind: ContentsKind): number {
        let size = Resolution.getChartSize(page);
        let bucketCount = Resolution.maxBucketCount;
        if (size.width / Resolution.minBarWidth < bucketCount)
            bucketCount = size.width / Resolution.minBarWidth;
        if (columnKind == "Integer" ||
            columnKind == "Category") {
            bucketCount = Math.min(bucketCount, stats.max - stats.min + 1);
        }
        return bucketCount;
    }

    public static categoriesInRange(stats: BasicColStats, bucketCount: number, allStrings: string[]): string[] {
        let boundaries: string[] = null;
        let max = Math.floor(stats.max);
        let min = Math.ceil(stats.min);
        let range = max - min;
        if (range <= 0)
            bucketCount = 1;

        if (allStrings != null) {
            if (bucketCount >= range) {
                boundaries = allStrings.slice(min, max + 1);  // slice end is exclusive
            } else {
                boundaries = [];
                for (let i = 0; i <= bucketCount; i++) {
                    let index = min + Math.round(i * range / bucketCount);
                    boundaries.push(allStrings[index]);
                }
            }
        }
        return boundaries;
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

export class BucketDialog extends Dialog {
    constructor() {
        super("Set buckets");
        this.addTextField("n_buckets", "Number of buckets:", "Integer");
    }

    getBucketCount(): number {
        return this.getFieldValueAsInt("n_buckets");
    }
}

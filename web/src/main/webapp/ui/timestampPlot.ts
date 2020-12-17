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

import {ContentsKind, Groups} from "../javaBridge";
import {D3SvgElement} from "./ui";
import {select as d3select} from "d3-selection";
import {ColorMap, Converters, formatNumber} from "../util";

/**
 * A Timestamp plot is a specialized form of histogram plot for use in log file visualizations.
 */
export class TimestampPlot {
    public barHeight: number;
    public max: number;  // maximum value in a bucket
    protected minTs: number; // minimum timestamp
    protected maxTs: number; // maximum timestamp
    protected kind: ContentsKind;
    protected data: Groups<number>;
    protected svgElement: D3SvgElement;
    protected readonly logicalHeight = 100;  // abstract svg units

    public constructor(protected parent: HTMLDivElement, protected map: ColorMap) {}

    public setHistogram(bars: Groups<number>, minTs: number, maxTs: number, kind: ContentsKind): void {
        this.data = bars;
        this.minTs = minTs;
        this.maxTs = maxTs;
        this.kind = kind;
        const bucketCount = this.data.perBucket.length;
        this.barHeight = this.logicalHeight / bucketCount;

        this.svgElement = d3select(this.parent)
            .append("svg")
            .attr("id", "canvas")
            .attr("border", 1)
            .attr("cursor", "crosshair")
            .attr("width", "100%")
            .attr("height", "100%")
            .attr("viewbox", "0 0 10 100")
            .attr("preserveAspectRatio", "none")
            .append("g");
    }

    public draw(): void {
        if (this.data == null)
            return;
        const counts = this.data.perBucket;
        const lineWidth = Converters.durationFromDouble((this.maxTs - this.minTs) / counts.length);
        this.max = Math.max(...counts);
        this.svgElement
            .selectAll("g")
            .data(counts)
            .enter()
            .append("rect")
            .attr("y", (d: number, i: number) => (i * this.barHeight) + "%")
            .attr("x", 0)
            .attr("fill", (d: number) => this.getColor(d))
            .attr("width", "100%")
            .attr("height", this.barHeight + "%") // since the box is 100
            .append("svg:title")
            .text((d: number, i: number) => this.getTs(i) + " " + lineWidth +
                " " + (d > 0 ? (", " + formatNumber(d) + " lines") : ""));
    }

    protected getTs(index: number): string {
        const interpolated = this.minTs + (this.maxTs - this.minTs) * index / this.data.perBucket.length;
        return Converters.valueToString(interpolated, this.kind, true)
    }

    getColor(value: number): string {
        if (value <= 0)
            return "white";
        if (this.max <= 0)
            return this.map(1);
        const result = this.map(value / this.max);
        // console.log(value + "," + result);
        return result;
    }
}

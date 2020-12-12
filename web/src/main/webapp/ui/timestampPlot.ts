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

import {scaleLinear as d3scaleLinear} from "d3-scale";
import {ContentsKind, Groups} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Scale} from "./ui";
import {ColorMap, Converters, formatNumber} from "../util";

/**
 * A Timestamp plot is a specialized form of histogram plot for use in log file visualizations.
 */
export class TimestampPlot extends Plot<Groups<number>> {
    public barHeight: number;
    protected yScale: D3Scale;
    public max: number;  // maximum value in a bucket
    protected chartWidth: number;
    protected minTs: number; // minimum timestamp
    protected maxTs: number; // maximum timestamp
    protected kind: ContentsKind;

    public constructor(protected plottingSurface: PlottingSurface, protected map: ColorMap) {
        super(plottingSurface);
    }

    public setHistogram(bars: Groups<number>, minTs: number, maxTs: number, kind: ContentsKind): void {
        this.data = bars;
        this.minTs = minTs;
        this.maxTs = maxTs;
        this.kind = kind;
        const chartHeight = this.getChartHeight();
        this.chartWidth = this.getChartWidth();
        const bucketCount = this.data.perBucket.length;
        this.barHeight = chartHeight / bucketCount;
    }

    public draw(): void {
        if (this.data == null)
            return;
        const counts = this.data.perBucket;
        this.max = Math.max(...counts);
        this.yScale = d3scaleLinear()
            .domain([0, this.max])
            .range([0, this.chartWidth]);
        this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(counts)
            .enter()
            .append("g")
            .append("rect")
            .attr("y", (d: number, i: number) => i * this.barHeight)
            .attr("x", 0)
            .attr("fill", (d: number) => this.getColor(d))
            .attr("width", this.chartWidth)
            .attr("height", this.barHeight)
            .append("svg:title")
            .text((d: number, i: number) => d > 0 ? (this.getTs(i) + ", " + formatNumber(d) + " lines") : "");
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
        return this.map(value / this.max);
    }
}

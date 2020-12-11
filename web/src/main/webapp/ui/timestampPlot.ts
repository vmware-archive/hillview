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
import {AxisKind} from "../dataViews/axisData";
import {Groups} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Scale} from "./ui";

/**
 * A Timestamp plot is a specialized form of histogram plot for use in log file visualizations.
 */
export class TimestampPlot extends Plot<Groups<number>> {
    public barHeight: number;
    protected yScale: D3Scale;
    public max: number;  // maximum value in a bucket
    protected chartWidth: number;

    public constructor(protected plottingSurface: PlottingSurface, protected color: string) {
        super(plottingSurface);
    }

    public setHistogram(bars: Groups<number>): void {
        this.data = bars;
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
            .attr("x", (d: number) => this.chartWidth - this.getWidth(d))
            .attr("fill", (d: number) => d == 0 ? "lightgrey" : this.color)
            .attr("width", (d: number) => { const w = this.getWidth(d); console.log(w); return w; })
            .attr("height", this.barHeight);
    }

    getWidth(c: number): number {
        if (c == 0)
            return this.chartWidth;
        return this.yScale(c);
    }

    /**
     * The index of the bucket covering the current x position on the X axis.
     */
    public getBucketIndex(x: number): number {
        const bucket = Math.floor(x / this.barHeight);
        if (bucket < 0 || this.data == null ||
            bucket >= this.data.perBucket.length)
            return -1;
        return bucket;
    }
}

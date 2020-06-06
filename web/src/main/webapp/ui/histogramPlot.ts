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

import {zip as d3zip} from "d3-array";
import {axisLeft as d3axisLeft} from "d3-axis";
import {format as d3format} from "d3-format";
import {scaleLinear as d3scaleLinear} from "d3-scale";
import {AxisData, AxisDescription, AxisKind} from "../dataViews/axisData";
import {Groups} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Axis, D3Scale} from "./ui";
import {symbol, symbolTriangle} from "d3-shape";
import {Two, valueWithConfidence} from "../util";
import {IBarPlot} from "./IBarPlot";

/**
 * A HistogramPlot draws a bar chart on a PlottingSurface, including the axes.
 */
export class HistogramPlot extends Plot implements IBarPlot {
    // While the data has histogram type, nothing prevents the values in the histogram
    // from being non-integers, so this class can be used to draw more general bar-charts.

    /**
     * Histogram that is being drawn.
     */
    public data: Two<Groups<number>>;
    /**
     * Sampling rate that was used to compute the histogram.
     */
    public samplingRate: number;
    public barWidth: number;
    protected yScale: D3Scale;
    protected yAxis: D3Axis;
    // Maximum value to use for the Y axis.
    public maxYAxis: number | null;
    public max: number;  // maximum value in a bucket
    public displayAxes: boolean;
    public isPrivate: boolean;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
        this.displayAxes = true;
    }

    /**
     * Set the histogram that we want to draw.
     * @param bars          Description of the histogram bars.
     * @param samplingRate  Sampling rate used to compute this histogram.
     * @param axisData      Description of the X axis; can have more buckets than the histogram,
     *                      since the same data may be used for a CDF plot.
     * @param maxYAxis      If present it is used to scale the maximum value for the Y axis.
     * @param isPrivate     True if we are plotting private data.
     */
    public setHistogram(bars: Two<Groups<number>>, samplingRate: number,
                        axisData: AxisData, maxYAxis: number | null, isPrivate: boolean): void {
        // TODO: display missing data graphically.
        this.data = bars;
        this.samplingRate = samplingRate;
        this.xAxisData = axisData;
        const chartWidth = this.getChartWidth();
        const bucketCount = this.data.first.perBucket.length;
        this.barWidth = chartWidth / bucketCount;
        this.maxYAxis = maxYAxis;
        this.isPrivate = isPrivate;
    }

    private confident(valueAndConf: number[]): boolean {
        if (!this.isPrivate)
            return true;
        return valueAndConf[0] > 2 * valueAndConf[1];
    }

    private drawBars(): void {
        const counts = this.data.first.perBucket.map((x) => Math.max(x, 0));
        this.max = Math.max(...counts);
        const displayMax = this.maxYAxis == null ? this.max : this.maxYAxis;
        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();
        const confidence = this.isPrivate ? this.data.second :
            new Array(this.data.first.perBucket.length); // filled with zeros
        const zippedData = d3zip(counts, confidence);
        const bars = this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(zippedData)
            .enter().append("g")
            .attr("transform", (d, i) => `translate(${i * this.barWidth}, 0)`);

        this.yScale = d3scaleLinear()
            .domain([0, displayMax])
            .range([chartHeight, 0]);

        // Boxes can be taller than the maxYAxis height.  In this case yScale returns
        // a negative value, and we have to truncate the rectangles.
        bars.append("rect")
            .attr("y", (d) => this.yScale(d[0]) < 0 ? 0 : this.yScale(d[0]))
            .attr("fill", (d) => this.confident(d) ? "darkcyan" : "lightgrey")
            .attr("height", (d) => chartHeight - this.yLabel(d[0]))
            .attr("width", this.barWidth - 1);

        // overflow signs if necessary
        bars.append("svg:path")
            // I am not sure how triangle size is measured; this 7 below seems to work find
            .attr("d", symbol().type(symbolTriangle).size( (d) => this.yScale(d[0]) < 0 ? 7 * this.barWidth : 0))
            .attr("transform", () => `translate(${this.barWidth / 2}, 0)`)
            .style("fill", "red")
            .append("svg:title")
            .text("Bar is truncated");

        if (this.isPrivate) {
            // confidence intervals
            bars.append("line")
                .attr("x1", 0)
                .attr("y1", (d) => this.yScale(d[0] - d[1]))
                .attr("x2", 0)
                .attr("y2", (d) => this.yScale(d[0] + d[1]))
                .attr("stroke-width", 1)
                .attr("stroke", "black")
                .attr("stroke-linecap", "round")
                .attr("transform", () => `translate(${this.barWidth / 2}, 0)`);
        }

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", this.barWidth / 2)
            .attr("y", (d) => this.yLabel(d[0]))
            .attr("text-anchor", "middle")
            .attr("dy", (d) => d[0] <= (9 * displayMax / 10) ? "-.25em" : ".75em")
            .text((d) => Plot.boxHeight(
                d[0], this.samplingRate, this.xAxisData.displayRange.presentCount))
            .exit();

        this.yAxis = d3axisLeft(this.yScale)
            .tickFormat(d3format(".2s"));

        this.xAxisData.setResolution(chartWidth, AxisKind.Bottom, PlottingSurface.bottomMargin);
    }

    private yLabel(value: number): number {
        const scale = this.yScale(value);
        if (scale < 0)
            return 0;
        return scale;
    }

    public draw(): void {
        if (this.data == null)
            return;

        this.drawBars();
        if (this.displayAxes)
            this.drawAxes();
    }

    public getYAxis(): AxisDescription {
        return new AxisDescription(this.yAxis, 1, false, null);
    }

    public getYScale(): D3Scale {
        return this.yScale;
    }

    /**
     * The index of the bucket covering the current x position on the X axis.
     */
    public getBucketIndex(x: number): number {
        const bucket = Math.floor(x / this.barWidth);
        if (bucket < 0 || this.data == null ||
            bucket >= this.data.first.perBucket.length)
            return -1;
        return bucket;
    }

    public get(x: number): [number, number] {
        const bucket = this.getBucketIndex(x);
        if (bucket < 0)
            return valueWithConfidence(0, null);
        const value = this.data.first.perBucket[bucket];
        const conf = this.isPrivate ? this.data.second.perBucket[bucket] : null;
        return valueWithConfidence(value, conf);
    }
}

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

import {axisLeft as d3axisLeft} from "d3-axis";
import {format as d3format} from "d3-format";
import {scaleLinear as d3scaleLinear} from "d3-scale";
import {AxisData, AxisDescription, AxisKind} from "../dataViews/axisData";
import {HistogramBase} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Axis, D3Scale} from "./ui";
import {symbol, symbolTriangle} from "d3-shape";

/**
 * A HistogramPlot draws a  bar chart on a PlottingSurface, including the axes.
 */
export class HistogramPlot extends Plot {
    // While the data has histogram type, nothing prevents the values in the histogram
    // from being non-integers, so this class can be used to draw more general bar-charts.

    /**
     * Histogram that is being drawn.
     */
    public histogram: HistogramBase;
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

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
        this.displayAxes = true;
    }

    /**
     * Set the histogram that we want to draw.
     * @param bars          Description of the histogram bars.
     * @param samplingRate  Sampling rate used to compute this histogram.
     * @param axisData      Description of the X axis.
     * @param maxYAxis      If present it is used to scale the maximum value for the Y axis.
     */
    public setHistogram(bars: HistogramBase, samplingRate: number,
                        axisData: AxisData, maxYAxis: number | null): void {
        this.histogram = bars;
        this.samplingRate = samplingRate;
        this.xAxisData = axisData;
        const chartWidth = this.getChartWidth();
        const bucketCount = this.histogram.buckets.length;
        this.barWidth = chartWidth / bucketCount;
        this.maxYAxis = maxYAxis;
    }

    public draw(): void {
        if (this.histogram == null)
            return;

        const counts = this.histogram.buckets;
        this.max = Math.max(...counts);
        const displayMax = this.maxYAxis == null ? this.max : this.maxYAxis;

        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();

        const bars = this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => `translate(${i * this.barWidth}, 0)`);

        this.yScale = d3scaleLinear()
            .domain([0, displayMax])
            .range([chartHeight, 0]);

        // Boxes can be taller than the maxYAxis height.  In this cale yScale returns
        // a negative value, and we have to truncate the rectangles.
        bars.append("rect")
            .attr("y", (d) => this.yScale(d) < 0 ? 0 : this.yScale(d))
            .attr("fill", "darkcyan")
            .attr("height", (d) => chartHeight - (this.yScale(d) < 0 ? 0 : this.yScale(d)))
            .attr("width", this.barWidth - 1);

        // overflow signs if necessary
        bars.append("svg:path")
            // I am not sure how triangle size is measured; this 7 below seems to work find
            .attr("d", symbol().type(symbolTriangle).size( (d) => this.yScale(d) < 0 ? 7 * this.barWidth : 0))
            .attr("transform", () => `translate(${this.barWidth / 2}, 0)`)
            .style("fill", "red")
            .append("svg:title")
            .text("Bar is truncated");

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", this.barWidth / 2)
            .attr("y", (d) => this.yScale(d) < 0 ? 0 : this.yScale(d))
            .attr("text-anchor", "middle")
            .attr("dy", (d) => d <= (9 * displayMax / 10) ? "-.25em" : ".75em")
            .text((d) => HistogramPlot.boxHeight(
                d, this.samplingRate, this.xAxisData.range.presentCount))
            .exit();

        this.yAxis = d3axisLeft(this.yScale)
            .tickFormat(d3format(".2s"));

        this.xAxisData.setResolution(chartWidth, AxisKind.Bottom, PlottingSurface.bottomMargin);
        if (this.displayAxes)
            this.drawAxes();
    }

    public getYAxis(): AxisDescription {
        return new AxisDescription(this.yAxis, 1, false, null);
    }

    public getYScale(): D3Scale {
        return this.yScale;
    }

    public get(x: number): number {
        const bucket = Math.floor(x / this.barWidth);
        if (bucket < 0 || bucket >= this.histogram.buckets.length)
            return 0;
        const value = this.histogram.buckets[bucket];
        return value;
    }
}

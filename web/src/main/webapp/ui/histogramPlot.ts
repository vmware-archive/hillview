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

import {PlottingSurface} from "./plottingSurface";
import {Histogram} from "../javaBridge";
import {HistogramViewBase} from "../dataViews/histogramViewBase";
import {Plot} from "./plot";
import {AxisData} from "../dataViews/axisData";
import {scaleLinear as d3scaleLinear} from "d3-scale";
import {axisLeft as d3axisLeft} from "d3-axis";
import {format as d3format} from "d3-format";

/**
 * A HistogramPlot draws a  bar chart on a PlottingSurface, including the axes.
 */
export class HistogramPlot extends Plot {
    /**
     * Histogram that is being drawn.
     */
    histogram: Histogram;
    /**
     * Sampling rate that was used to compute the histogram.
     */
    samplingRate: number;
    /**
     * Data used to draw the X axis.
     */
    axisData: AxisData;
    barWidth: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    // While the data has histogram type, nothing prevents the values in the histogram
    // from being non-integers, so this class can be used to draw more general bar-charts.
    public setHistogram(bars: Histogram, samplingRate: number, axisData: AxisData): void {
        this.histogram = bars;
        this.samplingRate = samplingRate;
        this.axisData = axisData;
        let chartWidth = this.getChartWidth();
        let bucketCount = this.histogram.buckets.length;
        this.barWidth = chartWidth / bucketCount;
    }

    public draw(): void {
        this.plottingSurface.clear();
        if (this.histogram == null)
            return;

        let counts = this.histogram.buckets;
        let bucketCount = counts.length;
        let max = Math.max(...counts);

        let chartWidth = this.getChartWidth();
        let chartHeight = this.getChartHeight();

        let bars = this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => `translate(${i * this.barWidth}, 0)`);

        this.yScale = d3scaleLinear()
            .domain([0, max])
            .range([chartHeight, 0]);

        bars.append("rect")
            .attr("y", d => this.yScale(d))
            .attr("fill", "darkcyan")
            .attr("height", d => chartHeight - this.yScale(d))
            .attr("width", this.barWidth - 1);

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", this.barWidth / 2)
            .attr("y", d => this.yScale(d))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text(d => HistogramViewBase.boxHeight(d, this.samplingRate, this.axisData.stats.presentCount))
            .exit();

        this.yAxis = d3axisLeft(this.yScale)
            .tickFormat(d3format(".2s"));

        let scaleAxis = this.axisData.scaleAndAxis(chartWidth, true, false);
        this.xScale = scaleAxis.scale;
        this.xAxis = scaleAxis.axis;

        this.drawAxes();
    }

    public get(x: number): number {
        let bucket = Math.floor(x / this.barWidth);
        if (bucket < 0 || bucket >= this.histogram.buckets.length)
            return 0;
        let value = this.histogram.buckets[bucket];
        return value;
    }
}


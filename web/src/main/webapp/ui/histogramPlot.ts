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
import {d3} from "./d3-modules";
import {Plot} from "./plot";
import {AxisData} from "../dataViews/axisData";

/**
 * A HistogramPlot draws a 1D histogram on a PlottingSurface, including the axes.
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

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public setHistogram(histogram: Histogram, samplingRate: number, axisData: AxisData): void {
        this.histogram = histogram;
        this.samplingRate = samplingRate;
        this.axisData = axisData;
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

        let barWidth = chartWidth / bucketCount;
        let bars = this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(counts)
            .enter().append("g")
            .attr("transform", (d, i) => `translate(${i * barWidth}, 0)`);

        this.yScale = d3.scaleLinear()
            .domain([0, max])
            .range([chartHeight, 0]);

        bars.append("rect")
            .attr("y", d => this.yScale(d))
            .attr("fill", "darkcyan")
            .attr("height", d => chartHeight - this.yScale(d))
            .attr("width", barWidth - 1);

        bars.append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", barWidth / 2)
            .attr("y", d => this.yScale(d))
            .attr("text-anchor", "middle")
            .attr("dy", d => d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text(d => HistogramViewBase.boxHeight(d, this.samplingRate, this.axisData.stats.presentCount))
            .exit();

        this.yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let scaleAxis = this.axisData.scaleAndAxis(chartWidth, true, false);
        this.xScale = scaleAxis.scale;
        this.xAxis = scaleAxis.axis;

        this.drawAxes();
    }
}


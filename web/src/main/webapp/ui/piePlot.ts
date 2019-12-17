/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {pie as d3pie, arc as d3arc} from "d3-shape";
import {AxisData} from "../dataViews/axisData";
import {AugmentedHistogram} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Scale} from "./ui";
import {IBarPlot} from "./IBarPlot";

/**
 * A PiePlot draws a histogram as a pie chart on a PlottingSurface.
 */
export class PiePlot extends Plot implements IBarPlot {
    /**
     * Histogram that is being drawn.
     */
    public histogram: AugmentedHistogram;
    /**
     * Sampling rate that was used to compute the histogram.
     */
    public samplingRate: number;
    public isPrivate: boolean;
    private missingCount: number;
    public maxYAxis: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    /**
     * Set the histogram that we want to draw.
     * @param bars          Description of the histogram bars.
     * @param samplingRate  Sampling rate used to compute this histogram.
     * @param missingCount  Number of missing values.
     * @param axisData      Description of the X axis.
     * @param maxYAxis      Not used for pie chart.
     * @param isPrivate     True if we are plotting private data.
     */
    public setHistogram(bars: AugmentedHistogram, samplingRate: number, missingCount: number,
                        axisData: AxisData, maxYAxis: number | null, isPrivate: boolean): void {
        this.histogram = bars;
        this.samplingRate = samplingRate;
        this.xAxisData = axisData;
        this.isPrivate = isPrivate;
        this.missingCount = missingCount;
        this.maxYAxis = maxYAxis;
    }

    private drawPie(): void {
        // TODO: draw missing data
        // TODO: label arcs
        const counts = this.histogram.histogram.buckets;
        const pie = d3pie().sort(null);
        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();
        const arc = d3arc()
            .innerRadius(0)
            .outerRadius(Math.min(chartWidth, chartHeight) / 2.2);

        const tr = 'translate(' + (chartWidth / 2) + "," + (chartHeight / 2) + ')';
        this.plottingSurface
            .getChart()
            .selectAll("g")
            .data(pie(counts))
            .enter()
            .append("g")
            .attr('transform', tr)
            .append("path")
            .attr("d", arc)
            .attr('fill', (d,i) => Plot.colorMap(i / counts.length))
            .append("svg:title")
            .text((d,i) => this.xAxisData.bucketDescription(i, 40));
    }

    public draw(): void {
        if (this.histogram == null)
            return;

        this.drawPie();
    }

    // These should not be called.
    public getYScale(): D3Scale {
        return null;
    }

    public get(x: number): [number, number] {
        return null;
    }
}

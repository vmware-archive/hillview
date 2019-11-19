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

/**
 * A PiePlot draws a histogram as a pie chart on a PlottingSurface.
 */
export class PiePlot extends Plot {
    /**
     * Histogram that is being drawn.
     */
    public histogram: AugmentedHistogram;
    /**
     * Sampling rate that was used to compute the histogram.
     */
    public samplingRate: number;
    public isPrivate: boolean;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    /**
     * Set the histogram that we want to draw.
     * @param bars          Description of the histogram bars.
     * @param samplingRate  Sampling rate used to compute this histogram.
     * @param axisData      Description of the X axis.
     * @param maxYAxis      If present it is used to scale the maximum value for the Y axis.
     * @param isPrivate     True if we are plotting private data.
     */
    public setHistogram(bars: AugmentedHistogram, samplingRate: number,
                        axisData: AxisData, maxYAxis: number | null, isPrivate: boolean): void {
        this.histogram = bars;

        this.samplingRate = samplingRate;
        this.xAxisData = axisData;
        this.isPrivate = isPrivate;
    }

    private drawPie(): void {
        const counts = this.histogram.histogram.buckets;
        const arcs = d3pie().sort(null)(counts);
        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();
        const arc = d3arc()
            .innerRadius(0)
            .outerRadius(Math.min(chartWidth, chartHeight) / 2.2);
        this.plottingSurface
            .getChart()
            .selectAll("g")
            .selectAll("path")
            .data(arcs)
            .join("path")
            .attr("d", arc)
            .append("title");
    }

    public draw(): void {
        if (this.histogram == null)
            return;

        this.drawPie();
    }

    // The following methods should never be called

    public maxYAxis: number = 0;

    public getYScale(): D3Scale {
        return null;
    }

    public get(x: number): [number, number] {
        return null;
    }
}

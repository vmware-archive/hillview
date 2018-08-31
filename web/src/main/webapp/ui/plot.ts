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
import {AxisData} from "../dataViews/axisData";
import {D3Axis, D3SvgElement, SpecialChars} from "./ui";
import {interpolateRainbow as d3interpolateRainbow} from "d3-scale-chromatic";
import {significantDigits} from "../util";

/**
 * Abstract base class for all plots.
 * A plot just contains an image, but no event handling.
 * The event handling and interactive display is handled by *View classes.
 * Each *View class will usually contain one or more plots.
 * Multiple plots can share the same plotting surface.
 */
export abstract class Plot {
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected xAxisRepresentation: D3SvgElement;
    protected yAxisRepresentation: D3SvgElement;

    /**
     * Create a plot that will do all its drawing on the specified plotting surface.
     */
    protected constructor(protected plottingSurface: PlottingSurface) {}

    /**
     * Returns the chart width in pixels - excluding borders.
     */
    public getChartWidth(): number {
        return this.plottingSurface.getChartWidth();
    }

    /**
     * Returns the chart height in pixels - excluding borders.
     */
    public getChartHeight(): number {
        return this.plottingSurface.getChartHeight();
    }

    public clear(): void {
        this.plottingSurface.create();
    }

    public getXAxis(): D3Axis {
        // default implementation
        return this.xAxisData.axis;
    }

    public getYAxis(): D3Axis {
        // default implementation
        return this.yAxisData.axis;
    }

    /**
     * Draw a border around the plotting area with the specified width in pixels.
     */
    public border(width: number): void {
        this.plottingSurface.getChart()
            .append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", this.getChartWidth())
            .attr("height", this.getChartHeight())
            .attr("stroke", "black")
            .style("fill", "none")
            .attr("stroke-width", width);
    }

    protected drawAxes(): void {
        const yAxis = this.getYAxis();
        const xAxis = this.getXAxis();
        if (yAxis != null)
            this.yAxisRepresentation = this.plottingSurface.getChart()
                .append("g")
                .attr("class", "y-axis")
                .call(yAxis);
        if (xAxis != null) {
            this.xAxisRepresentation = this.plottingSurface.getChart()
                .append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(0, ${this.getChartHeight()})`)
                .call(xAxis);
        }
    }

    /**
     * Measure the maximum label width on a axis, in pixels.
     */
    protected labelWidth(): number {
        if (this.yAxisRepresentation == null)
            return 0;
        let max = 0;
        const domNodes = this.yAxisRepresentation.selectAll(".tick").nodes();
        for (const domNode of domNodes)
            max = Math.max(max, domNode.getBBox().width);
        return max;
    }

    public static colorMap(d: number): string {
        // The rainbow color map starts and ends with a similar hue
        // so we skip the first 20% of it.
        return d3interpolateRainbow(d * .8 + .2);
    }

    /**
     * Compute the string used to display the height of a box in a histogram
     * @param  barSize       Bar size as reported by histogram.
     * @param  samplingRate  Sampling rate that was used to compute the box height.
     * @param  rowCount      Total population which was sampled to get this box.
     */
    public static boxHeight(barSize: number, samplingRate: number, rowCount: number): string {
        if (samplingRate >= 1) {
            if (barSize === 0)
                return "";
            return significantDigits(barSize);
        }
        const muS = barSize / rowCount;
        const dev = 2.38 * Math.sqrt(muS * (1 - muS) * rowCount / samplingRate);
        const min = Math.max(barSize - dev, 0);
        const max = barSize + dev;
        const minString = significantDigits(min);
        const maxString = significantDigits(max);
        if (minString === maxString && dev !== 0)
            return minString;
        return SpecialChars.approx + significantDigits(barSize);
    }

    public abstract draw(): void;
}

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

/**
 * Abstract base class for all plots.
 * A plot just contains an image, but no event handling.
 * The event handling and interactive display is handled by *View classes.
 * Each *View class will usually contain one or more plots.
 * Multiple plots can share the same plotting surface.
 */
export abstract class Plot {
    /**
     * d3 Scale used for Y axis.
     */
    public yScale: any;
    /**
     * D3 vertical axis.
     */
    public yAxis: any;
    /**
     * d3 Scale used for X axis.
     */
    public xScale: any;
    /**
     * D3 vertical axis.
     */
    public xAxis: any;
    protected xAxisRepresentation: any;
    protected yAxisRepresentation: any;

    /**
     * Create a plot that will do all its drawing on the specified plotting surface.
     */
    protected constructor(protected plottingSurface: PlottingSurface) {}

    /**
     * When the plot is of a chart this returns the chart width in pixels - excluding borders.
     */
    getChartWidth(): number {
        return this.plottingSurface.getActualChartWidth();
    }

    /**
     * When the plot is of a chart this returns the chart height in pixels - excluding borders.
     */
    getChartHeight(): number {
        return this.plottingSurface.getActualChartHeight();
    }

    public clear(): void {
        this.plottingSurface.clear();
    }

    drawAxes(): void {
        if (this.yAxis != null)
            this.yAxisRepresentation = this.plottingSurface.getChart()
                .append("g")
                .attr("class", "y-axis")
                .call(this.yAxis);
        if (this.xAxis != null) {
            this.xAxisRepresentation = this.plottingSurface.getChart()
                .append("g")
                .attr("class", "x-axis")
                .attr("transform", `translate(0, ${this.getChartHeight()})`)
                .call(this.xAxis);
        }
    }

    /**
     * Measure the maximum label width on a axis, in pixels.
     */
    labelWidth(): number {
        if (this.yAxis == null)
            return 0;
        let max = 0;
        let domNodes = this.yAxisRepresentation.selectAll(".tick").select("text").nodes();
        for (let i = 0; i < domNodes.length; i++)
            max = Math.max(max, domNodes[i].getBBox().width);
        return max;
    }

    public abstract draw(): void;
}
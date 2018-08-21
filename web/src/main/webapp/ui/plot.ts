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
import {D3Axis, D3SvgElement} from "./ui";

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
     * When the plot is of a chart this returns the chart width in pixels - excluding borders.
     */
    public getChartWidth(): number {
        return this.plottingSurface.getActualChartWidth();
    }

    /**
     * When the plot is of a chart this returns the chart height in pixels - excluding borders.
     */
    public getChartHeight(): number {
        return this.plottingSurface.getActualChartHeight();
    }

    public clear(): void {
        this.plottingSurface.clear();
    }

    protected getXAxis(): D3Axis {
        // default implementation
        return this.xAxisData.axis;
    }

    protected getYAxis(): D3Axis {
        // default implementation
        return this.yAxisData.axis;
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

    public abstract draw(): void;
}

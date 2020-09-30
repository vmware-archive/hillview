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

import {AxisData, AxisDescription, AxisKind} from "../dataViews/axisData";
import {Groups} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Axis, D3Scale} from "./ui";
import {SchemaClass} from "../schemaClass";
import {ColorMap, Pair} from "../util";

/**
 * Draws a histogram with stacked bars.
 */
export abstract class Histogram2DBase extends Plot<Pair<Groups<Groups<number>>, Groups<Groups<number>> | null>> {
    protected xAxisData: AxisData;
    protected samplingRate: number;
    protected normalized: boolean;
    protected barWidth: number;
    protected yScale: D3Scale;
    protected yAxis: D3Axis;
    protected schema: SchemaClass;
    public maxYAxis: number | null; // If not null the maximum value to display
    public max: number; // the maximum value in a stacked bar
    public colorMap: ColorMap;

    protected constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public setData(heatmap: Pair<Groups<Groups<number>>, Groups<Groups<number>> | null>,
                   xAxisData: AxisData,
                   samplingRate: number,
                   normalized: boolean,
                   schema: SchemaClass,
                   colorMap: ColorMap,
                   max: number | null,
                   rowCount: number): void {
        this.data = heatmap;
        this.xAxisData = xAxisData;
        this.samplingRate = samplingRate;
        this.normalized = normalized;
        this.schema = schema;
        this.maxYAxis = max;
        this.colorMap = colorMap;
        this.rowCount = rowCount;
    }

    public draw(): void {
        this.xAxisData.setResolution(this.getChartWidth(), AxisKind.Bottom, PlottingSurface.bottomMargin);
        // Axis legends
        this.plottingSurface.getCanvas().append("text")
            .text(this.xAxisData.getName())
            .attr("transform", `translate(${this.getChartWidth() / 2},
            ${this.getChartHeight() + this.plottingSurface.topMargin + this.plottingSurface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");
    }

    public getYAxis(): AxisDescription {
        return this.yAxis;
    }

    public getYScale(): D3Scale {
        return this.yScale;
    }

    /**
     * The bar width in pixels.
     */
    public getBarWidth(): number {
        return this.barWidth;
    }

    // noinspection JSMethodCanBeStatic
    protected color(d: number, max: number): string {
        if (d < 0 || d > max)
            // This is for the "missing" data
            return "none";
        if (max === 0)
            return this.colorMap(0);
        return this.colorMap(d / max);
    }
}

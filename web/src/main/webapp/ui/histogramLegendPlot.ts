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

import {AxisData, AxisKind} from "../dataViews/axisData";
import {Plot} from "./plot";
import {Resolution} from "./ui";
import {SchemaClass} from "../schemaClass";
import {LegendPlot} from "./legendPlot";
import {HtmlPlottingSurface} from "./plottingSurface";
import {ColorMap, desaturateOutsideRange} from "../util";
import {kindIsString} from "../javaBridge";
import {interpolateCool as d3interpolateCool, interpolateWarm as d3interpolateWarm} from "d3-scale-chromatic";
import {ColorMapKind} from "./heatmapLegendPlot";

/**
 * Displays a legend for a 2D histogram.
 */
export class HistogramLegendPlot extends LegendPlot<void> {
    protected axisData: AxisData;
    protected missingLegend: boolean;  // if true display legend for missing
    protected missingX: number;
    protected missingY: number;
    protected readonly missingGap = 30;
    protected readonly missingWidth = 20;
    protected colorWidth: number;
    protected schema: SchemaClass;
    public    colorMap: ColorMap;

    public constructor(surface: HtmlPlottingSurface, onSelectionCompleted: (xl: number, xr: number) => void) {
        super(surface, onSelectionCompleted);
        this.y = Resolution.legendSpaceHeight / 3;
        this.createRectangle();
    }

    public draw(): void {
        this.plottingSurface.getCanvas()
            .append("text")
            .text(this.axisData.getDisplayNameString(this.schema))
            .attr("transform", `translate(${this.getChartWidth() / 2}, 0)`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");

        let x = this.x;
        const canvas = this.plottingSurface.getCanvas();
        this.colorWidth = this.width / this.axisData.bucketCount;
        for (let i = 0; i < this.axisData.bucketCount; i++) {
            let color: string;
            if (this.axisData.bucketCount === 1)
                color = this.colorMap(0);
            else
                color = this.colorMap(i / (this.axisData.bucketCount - 1));
            canvas.append("rect")
                .attr("width", this.colorWidth)
                .attr("height", this.height)
                .style("fill", color)
                .attr("x", x)
                .attr("y", this.y)
                .append("title")
                .text(this.axisData.bucketDescription(i, 100));
            x += this.colorWidth;
        }

        this.axisData.setResolution(this.legendRect.width(), AxisKind.Legend, this.height);
        const g = canvas.append("g")
            .attr("transform", `translate(${this.legendRect.lowerLeft().x},
                                          ${this.legendRect.lowerLeft().y})`)
            .attr("class", "x-axis");
        this.axisData.axis.draw(g);

        if (this.missingLegend) {
            if (this.legendRect != null) {
                this.missingX = this.legendRect.upperRight().x + this.missingGap;
                this.missingY = this.legendRect.upperRight().y;
            } else {
                this.missingX = this.getChartWidth() / 2;
                this.missingY = Resolution.legendSpaceHeight / 3;
            }

            canvas.append("rect")
                .attr("width", this.missingWidth)
                .attr("height", this.height)
                .attr("x", this.missingX)
                .attr("y", this.missingY)
                .attr("stroke", "black")
                .attr("fill", "none")
                .attr("stroke-width", 1);

            canvas.append("text")
                .text("missing")
                .attr("transform", `translate(${this.missingX + this.missingWidth / 2},
                                              ${this.missingY + this.height + 7})`)
                .attr("text-anchor", "middle")
                .attr("font-size", 10)
                .attr("dominant-baseline", "text-before-edge");
        }
        super.draw();
    }

    public setColorMapKind(kind: ColorMapKind): void {
        if (kindIsString(this.axisData.description.kind))
            // keep the existing one: categorical.
            return;
        switch (kind) {
            case ColorMapKind.Cool:
                this.colorMap = d3interpolateCool;
                break;
            case ColorMapKind.Warm:
                this.colorMap = d3interpolateWarm;
                break;
            case ColorMapKind.Grayscale:
                this.colorMap = (x: number) => `rgb(
                ${Math.round(255 * (1 - x))},${Math.round(255 * (1 - x))},${Math.round(255 * (1 - x))})`;
                break;
        }
    }

    /**
     * Highlight the color with the specified index.  Special values:
     * - colorIndex is bucketCount: missing box
     * - colorIndex is null: nothing
     */
    public highlight(colorIndex: number | null): void {
        if (colorIndex == null) {
            this.hilightRect
                .attr("width", 0);
        } else if (colorIndex == this.axisData.bucketCount && this.missingLegend) {
            this.hilightRect
                .attr("x", this.missingX)
                .attr("y", this.missingY)
                .attr("width", this.missingWidth);
        } else {
            this.hilightRect
                .attr("width", this.colorWidth)
                .attr("x", this.x + colorIndex * this.colorWidth)
                .attr("y", this.y);
        }
    }

    public getColorMap(): ColorMap {
        return (x) => this.colorMap(x / this.axisData.bucketCount);
    }

    public setData(axis: AxisData, missingLegend: boolean, schema: SchemaClass): void {
        this.axisData = axis;
        this.missingLegend = missingLegend;
        this.schema = schema;
        if (kindIsString(axis.description.kind))
            this.colorMap = (d) => Plot.categoricalMap(Math.round(d * (this.axisData.bucketCount - 1)));
        else
            this.colorMap = Plot.defaultColorMap;
    }

    /**
     * Emphasize the colors in the map in range x0 to x1.  These are
     * two values in the range 0-1.
     */
    public emphasizeRange(x0: number, x1: number): void {
        this.colorMap = desaturateOutsideRange(
            Plot.defaultColorMap, x0, x1);
    }

    public setSurface(surface: HtmlPlottingSurface): void {
        this.plottingSurface = surface;
    }
}


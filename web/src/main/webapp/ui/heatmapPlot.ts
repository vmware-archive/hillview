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
import {Heatmap, kindIsString} from "../javaBridge";
import {regression} from "../util";
import {HeatmapLegendPlot} from "./legendPlot";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {SchemaClass} from "../schemaClass";

interface Dot {
    x: number;
    y: number;
    v: number;
}

export class HeatmapPlot extends Plot {
    protected heatmap: Heatmap;
    protected pointWidth: number; // in pixels
    protected pointHeight: number; // in pixels
    protected max: number;  // maximum count
    protected visible: number;
    protected distinct: number;
    protected dots: Dot[];
    protected schema: SchemaClass;

    public constructor(surface: PlottingSurface,
                       protected legendPlot: HeatmapLegendPlot,
                       protected showAxes: boolean) {
        super(surface);
        this.dots = null;
    }

    public draw(): void {
        if (this.dots == null) {
            this.plottingSurface.reportError("No data to display");
            return;
        }

        const canvas = this.plottingSurface.getCanvas();
        if (this.showAxes) {
            canvas.append("text")
                .text(this.schema.displayName(this.yAxisData.description.name))
                .attr("dominant-baseline", "text-before-edge");
            canvas.append("text")
                .text(this.schema.displayName(this.xAxisData.description.name))
                .attr("x", this.getChartWidth() / 2)
                .attr("y", this.getChartHeight() + this.plottingSurface.topMargin +
                        this.plottingSurface.bottomMargin / 2)
                .attr("text-anchor", "middle")
                .attr("dominant-baseline", "hanging");
        }

        const htmlCanvas: HTMLCanvasElement = document.createElement("canvas");
        htmlCanvas.height = this.getChartHeight();
        htmlCanvas.width = this.getChartWidth();
        // draw the image onto the canvas.
        const ctx: CanvasRenderingContext2D = htmlCanvas.getContext("2d");
        for (const dot of this.dots) {
            ctx.beginPath();
            ctx.fillStyle = this.legendPlot.getColor(dot.v);
            ctx.fillRect(dot.x, dot.y, this.pointWidth, this.pointHeight);
            ctx.closePath();
        }
        const url = htmlCanvas.toDataURL("image/png");
        this.plottingSurface.getChart()
            .append("image")
            .attr("xlink:href", url)
            .attr("width", this.getChartWidth())
            .attr("height", this.getChartHeight());
        if (this.showAxes)
            this.drawAxes();

        if (!kindIsString(this.yAxisData.description.kind) &&
            !kindIsString(this.xAxisData.description.kind)) {
            // it makes no sense to do regressions for string values
            const regr = regression(this.heatmap.buckets);
            if (regr.length === 2) {
                const b = regr[0];
                const a = regr[1];
                const y1 = this.getChartHeight() - (b + .5) * this.pointHeight;
                const y2 = this.getChartHeight() - (a * this.heatmap.buckets.length + b + .5) * this.pointHeight;
                this.plottingSurface.getChart()
                    .append("line")
                    .attr("x1", 0)
                    .attr("y1", y1)
                    .attr("x2", this.pointWidth * this.heatmap.buckets.length)
                    .attr("y2", y2)
                    .attr("stroke", "black");
            }
        }
    }

    /**
     * Given two screen coordinates within the chart, return the count displayed at that coordinate.
     */
    public getCount(x: number, y: number): number {
        let xi = x / this.pointWidth;
        let yi = (this.getChartHeight() - y) / this.pointHeight;
        xi = Math.floor(xi);
        yi = Math.floor(yi);
        const xPoints = this.heatmap.buckets.length;
        const yPoints = this.heatmap.buckets[0].length;
        if (xi >= 0 && xi < xPoints && yi >= 0 && yi < yPoints)
            return this.heatmap.buckets[xi][yi];
        return 0;
    }

    public getMaxCount(): number {
        return this.max;
    }

    public getVisiblePoints(): number {
        return this.visible;
    }

    public getDistinct(): number {
        return this.distinct;
    }

    public setData(heatmap: Heatmap, xData: AxisData, yData: AxisData, schema: SchemaClass): void {
        this.heatmap = heatmap;
        this.xAxisData = xData;
        this.yAxisData = yData;
        this.schema = schema;
        this.xAxisData.setResolution(this.getChartWidth(), AxisKind.Bottom);
        this.yAxisData.setResolution(this.getChartHeight(), AxisKind.Left);

        const xPoints = this.heatmap.buckets.length;
        const yPoints = this.heatmap.buckets[0].length;
        if (xPoints === 0 || yPoints === 0)
            return;
        this.pointWidth = this.getChartWidth() / xPoints;
        this.pointHeight = this.getChartHeight() / yPoints;

        this.max = 0;
        this.visible = 0;
        this.distinct = 0;
        this.dots = [];

        for (let x = 0; x < this.heatmap.buckets.length; x++) {
            for (let y = 0; y < this.heatmap.buckets[x].length; y++) {
                const v = this.heatmap.buckets[x][y];
                if (v > this.max)
                    this.max = v;
                if (v !== 0) {
                    const rec = {
                        x: x * this.pointWidth,
                        y: this.getChartHeight() - (y + 1) * this.pointHeight,  // +1 because it's the upper corner
                        v: v
                    };
                    this.visible += v;
                    this.distinct++;
                    this.dots.push(rec);
                }
            }
        }
    }
}

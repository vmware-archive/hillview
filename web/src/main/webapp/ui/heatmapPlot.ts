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

import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {HeatMap} from "../javaBridge";
import {AxisData} from "../dataViews/axisData";
import {regression} from "../util";
import {HeatmapLegendPlot} from "./legendPlot";

interface Dot {
    x: number,
    y: number,
    v: number
}

export class HeatmapPlot extends Plot {
    protected heatmap: HeatMap;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected samplingRate: number;
    protected pointWidth: number; // in pixels
    protected pointHeight: number; // in pixels
    protected max: number;  // maximum count
    protected visible: number;
    protected distinct: number;
    protected dots: Dot[];

    public constructor(surface: PlottingSurface, protected legendPlot: HeatmapLegendPlot) {
        super(surface);
        this.dots = null;
    }

    public draw(): void {
        if (this.dots == null) {
            this.plottingSurface.reportError("No data to display");
            return;
        }

        let canvas = this.plottingSurface.getCanvas();
        canvas.append("text")
            .text(this.yAxisData.description.name)
            .attr("dominant-baseline", "text-before-edge");
        canvas.append("text")
            .text(this.xAxisData.description.name)
            .attr("transform", `translate(${this.getChartWidth() / 2},
                  ${this.getChartHeight() + this.plottingSurface.topMargin + this.plottingSurface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "hanging");

        let xsc = this.xAxisData.scaleAndAxis(this.getChartWidth(), true, false);
        let ysc = this.yAxisData.scaleAndAxis(this.getChartHeight(), false, false);
        this.xAxis = xsc.axis;
        this.yAxis = ysc.axis;
        this.xScale = xsc.scale;
        this.yScale = ysc.scale;

        this.plottingSurface.getChart()
            .selectAll()
            .data(this.dots)
            .enter()
            .append("g")
            .append("svg:rect")
            .attr("class", "heatMapCell")
            .attr("x", d => d.x)
            .attr("y", d => d.y)
            .attr("data-val", d => d.v)
            .attr("width", this.pointWidth)
            .attr("height", this.pointHeight)
            .style("stroke-width", 0)
            .style("fill", d => this.legendPlot.getColor(d.v));

        this.drawAxes();

        if (this.yAxisData.description.kind != "Category") {
            // it makes no sense to do regressions for categorical values
            let regr = regression(this.heatmap.buckets);
            if (regr.length == 2) {
                let b = regr[0];
                let a = regr[1];
                let y1 = this.getChartHeight() - b * this.pointHeight;
                let y2 = this.getChartHeight() - (a * this.heatmap.buckets.length + b) * this.pointHeight;
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
        let xPoints = this.heatmap.buckets.length;
        let yPoints = this.heatmap.buckets[0].length;
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

    public reapplyColorMap() {
        this.plottingSurface.getChart()
            .selectAll(".heatMapCell")
            .datum(function() {return this.dataset;})
            .style("fill", d => this.legendPlot.getColor(d.val))
    }

    public setData(heatmap: HeatMap, xData: AxisData, yData: AxisData, samplingRate: number) {
        this.heatmap = heatmap;
        this.xAxisData = xData;
        this.yAxisData = yData;
        this.samplingRate = samplingRate;

        let xPoints = this.heatmap.buckets.length;
        let yPoints = this.heatmap.buckets[0].length;
        if (xPoints == 0 || yPoints == 0)
            return;
        this.pointWidth = this.getChartWidth() / xPoints;
        this.pointHeight = this.getChartHeight() / yPoints;

        this.max = 0;
        this.visible = 0;
        this.distinct = 0;
        this.dots = [];

        for (let x = 0; x < this.heatmap.buckets.length; x++) {
            for (let y = 0; y < this.heatmap.buckets[x].length; y++) {
                let v = this.heatmap.buckets[x][y];
                if (v > this.max)
                    this.max = v;
                if (v != 0) {
                    let rec = {
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
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

import {d3} from "./d3-modules";
import {Plot} from "./plot";
import {HeatMap, Histogram} from "../javaBridge";
import {PlottingSurface} from "./plottingSurface";
import {AxisData} from "../dataViews/axisData";
import {HistogramViewBase} from "../dataViews/histogramViewBase";
import {Histogram2DView} from "../dataViews/histogram2DView";

/**
 * Represents an SVG rectangle drawn on the screen.
 */
interface Rect {
    /**
     * Bucket index on the X axis.
     */
    xIndex: number;
    /**
     * Bucket index on the Y axis.
     */
    yIndex: number;
    /**
     * Count of items represented by the rectangle.
     */
    count: number;
    /**
     * Count of items below this rectangle.
     */
    countBelow: number;
}

export class Histogram2DPlot extends Plot {
    /**
     * A 2D histogram contains a heatmap for the data, a 1D Histogram for the misisng data,
     * a cdf, and a missing count.
     */
    protected heatmap: HeatMap;
    protected cdf: Histogram;
    protected xAxisData: AxisData;
    protected yAxisData: AxisData;
    protected samplingRate: number;
    protected normalized: boolean;
    protected missingDisplayed: number;
    protected visiblePoints: number;
    protected barWidth: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public setData(heatmap: HeatMap, cdf: Histogram,
                   xAxisData: AxisData, yAxisData: AxisData, samplingRate: number,
                   normalized: boolean): void {
        this.heatmap = heatmap;
        this.cdf = cdf;
        this.xAxisData = xAxisData;
        this.yAxisData = yAxisData;
        this.samplingRate = samplingRate;
        this.normalized = normalized;
    }

    public draw(): void {
        let xPoints = this.heatmap.buckets.length;
        let yPoints = this.heatmap.buckets[0].length;

        let counts: number[] = [];
        this.missingDisplayed = 0;
        this.visiblePoints = 0;

        let max = 0;
        let rects: Rect[] = [];
        for (let x = 0; x < this.heatmap.buckets.length; x++) {
            let yTotal = 0;
            for (let y = 0; y < this.heatmap.buckets[x].length; y++) {
                let v = this.heatmap.buckets[x][y];
                this.visiblePoints += v;
                if (v != 0) {
                    let rec: Rect = {
                        xIndex: x,
                        countBelow: yTotal,
                        yIndex: y,
                        count: v
                    };
                    rects.push(rec);
                }
                yTotal += v;
            }
            let v = this.heatmap.histogramMissingY.buckets[x];
            let rec: Rect = {
                xIndex: x,
                countBelow: yTotal,
                yIndex: this.heatmap.buckets[x].length,
                count: v
            };
            rects.push(rec);
            yTotal += v;
            this.missingDisplayed += v;
            if (yTotal > max)
                max = yTotal;
            counts.push(yTotal);
        }

        this.yScale = d3.scaleLinear()
            .range([this.getChartHeight(), 0]);
        if (this.normalized)
            this.yScale.domain([0, 100]);
        else
            this.yScale.domain([0, max]);
        this.yAxis = d3.axisLeft(this.yScale)
            .tickFormat(d3.format(".2s"));

        let bucketCount = xPoints;
        let scAxis = this.xAxisData.scaleAndAxis(this.getChartWidth(), true, false);
        this.xScale = scAxis.scale;
        this.xAxis = scAxis.axis;

        this.plottingSurface.getCanvas().append("text")
            .text(this.xAxisData.description.name)
            .attr("transform", `translate(${this.getChartWidth() / 2},
            ${this.getChartHeight() + this.plottingSurface.topMargin + this.plottingSurface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");

        this.barWidth = this.getChartWidth() / bucketCount;
        let scale = this.getChartHeight() / max;

        this.plottingSurface.getChart()
            .selectAll("g")
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", (d: Rect) => d.xIndex * this.barWidth)
            .attr("y", (d: Rect) => this.rectPosition(d, counts, scale))
            .attr("height", (d: Rect) => this.rectHeight(d, counts, scale))
            .attr("width", this.barWidth - 1)
            .attr("fill", (d: Rect) => this.color(d.yIndex, yPoints - 1))
            .attr("stroke", "black")
            .attr("stroke-width", (d: Rect) => d.yIndex > yPoints - 1 ? 1 : 0)
            .exit()
            // label bars
            .data(counts)
            .enter()
            .append("g")
            .append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", (c, i: number) => (i + .5) * this.barWidth)
            .attr("y", (d: number) => this.normalized ? 0 : this.getChartHeight() - (d * scale))
            .attr("text-anchor", "middle")
            .attr("dy", (d: number) => this.normalized ? 0 : d <= (9 * max / 10) ? "-.25em" : ".75em")
            .text((d: number) => HistogramViewBase.boxHeight(d, this.samplingRate, this.visiblePoints))
            .exit();

        let noX = 0;
        for (let y = 0; y < this.heatmap.histogramMissingX.buckets.length; y++)
            noX += this.heatmap.histogramMissingX.buckets[y];

        if (max <= 0) {
            this.plottingSurface.reportError("All values are missing: " + noX + " have no X value, "
                + this.heatmap.missingData + " have no X or Y value");
            return;
        }

        this.drawAxes();
    }

    /**
     * The bar width in pixels.
     */
    public getBarWidth(): number {
        return this.barWidth;
    }

    /**
     * The total count of missing values displayed as rectangles.
     */
    public getMissingDisplayed(): number {
        return this.missingDisplayed;
    }

    /**
     * The total count of points that correspond to displayed rectangles.
     */
    public getVisiblePoints(): number {
        return this.visiblePoints + this.missingDisplayed;
    }

    protected rectHeight(d: Rect, counts: number[], scale: number): number {
        if (this.normalized) {
            let c = counts[d.xIndex];
            if (c <= 0)
                return 0;
            return this.getChartHeight() * d.count / c;
        }
        return d.count * scale;
    }

    protected rectPosition(d: Rect, counts: number[], scale: number): number {
        let y = d.countBelow + d.count;
        if (this.normalized) {
            let c = counts[d.xIndex];
            if (c <= 0)
                return 0;
            return this.getChartHeight() * (1 - y / c);
        }
        return this.getChartHeight() - y * scale;
    }

    color(d: number, max: number): string {
        if (d > max)
        // This is for the "missing" data
            return "none";
        if (max == 0)
            return Histogram2DView.colorMap(0);
        return Histogram2DView.colorMap(d / max);
    }
}
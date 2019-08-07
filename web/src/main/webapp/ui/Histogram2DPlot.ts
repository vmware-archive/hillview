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

import {axisLeft as d3axisLeft} from "d3-axis";
import {format as d3format} from "d3-format";
import {scaleLinear as d3scaleLinear} from "d3-scale";
import {AxisData, AxisDescription, AxisKind} from "../dataViews/axisData";
import {Heatmap} from "../javaBridge";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Axis, D3Scale} from "./ui";
import {SchemaClass} from "../schemaClass";
import {symbol, symbolTriangle} from "d3-shape";

/**
 * Represents an SVG rectangle drawn on the screen.
 */
interface Box {
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
     * Sometimes used to represent the total count for the whole bar.
     */
    countBelow: number;
}

export class Histogram2DPlot extends Plot {
    protected heatmap: Heatmap;
    protected xAxisData: AxisData;
    protected samplingRate: number;
    protected normalized: boolean;
    protected missingDisplayed: number;
    protected visiblePoints: number;
    protected barWidth: number;
    protected yScale: D3Scale;
    protected yAxis: D3Axis;
    protected schema: SchemaClass;
    public maxYAxis: number | null; // If not null the maximum value to display
    public max: number; // the maximum value in a stacked bar

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public setData(heatmap: Heatmap,
                   xAxisData: AxisData,
                   samplingRate: number,
                   normalized: boolean,
                   schema: SchemaClass,
                   max: number | null): void {
        this.heatmap = heatmap;
        this.xAxisData = xAxisData;
        this.samplingRate = samplingRate;
        this.normalized = normalized;
        this.schema = schema;
        this.maxYAxis = max;
    }

    public draw(): void {
        const xPoints = this.heatmap.buckets.length;
        const yPoints = this.heatmap.buckets[0].length;

        const counts: number[] = [];
        this.missingDisplayed = 0;
        this.visiblePoints = 0;

        this.max = 0;
        const rects: Box[] = [];
        for (let x = 0; x < this.heatmap.buckets.length; x++) {
            let yTotal = 0;
            for (let y = 0; y < this.heatmap.buckets[x].length; y++) {
                const vis = this.heatmap.buckets[x][y];
                this.visiblePoints += vis;
                if (vis !== 0) {
                    const rect: Box = {
                        xIndex: x,
                        countBelow: yTotal,
                        yIndex: y,
                        count: vis
                    };
                    rects.push(rect);
                }
                yTotal += vis;
            }
            if (this.heatmap.histogramMissingY != null) {
                const v = this.heatmap.histogramMissingY.buckets[x];
                const rec: Box = {
                    xIndex: x,
                    countBelow: yTotal,
                    yIndex: this.heatmap.buckets[x].length,
                    count: v
                };
                rects.push(rec);
                yTotal += v;
                this.missingDisplayed += v;
            }
            if (yTotal > this.max)
                this.max = yTotal;
            counts.push(yTotal);
        }

        const displayMax = this.maxYAxis != null ? this.maxYAxis : this.max;
        this.yScale = d3scaleLinear()
            .range([this.getChartHeight(), 0]);
        if (this.normalized)
            this.yScale.domain([0, 100]);
        else
            this.yScale.domain([0, displayMax]);
        this.yAxis = new AxisDescription(
            d3axisLeft(this.yScale).tickFormat(d3format(".2s")), 1, false, null);

        const bucketCount = xPoints;
        this.xAxisData.setResolution(this.getChartWidth(), AxisKind.Bottom, PlottingSurface.bottomMargin);

        this.plottingSurface.getCanvas().append("text")
            .text(this.schema.displayName(this.xAxisData.description.name))
            .attr("transform", `translate(${this.getChartWidth() / 2},
            ${this.getChartHeight() + this.plottingSurface.topMargin + this.plottingSurface.bottomMargin / 2})`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");

        this.barWidth = this.getChartWidth() / bucketCount;
        const scale = displayMax <= 0 ? 1 : this.getChartHeight() / displayMax;

        this.plottingSurface.getChart()
            .selectAll("g")
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", (d: Box) => d.xIndex * this.barWidth)
            .attr("y", (d: Box) => this.rectPosition(d, counts, scale))
            .attr("height", (d: Box) => this.rectHeight(d, counts, scale))
            .attr("width", this.barWidth - 1)
            .attr("fill", (d: Box) => this.color(d.yIndex, yPoints - 1))
            .attr("stroke", "black")
            .attr("stroke-width", (d: Box) => d.yIndex > yPoints - 1 ? 1 : 0)
            .exit()
            // overflow signs if necessary
            .data(counts)
            .enter()
            .append("g")
            .append("svg:path")
            // I am not sure how triangle size is measured; this 7 below seems to work find
            .attr("d", symbol().type(symbolTriangle).size(
                (d: number) => (!this.normalized && ((d * scale) > this.getChartHeight())) ?
                    7 * this.barWidth : 0))
            .attr("transform", (c, i: number) => `translate(${(i + .5) * this.barWidth}, 0)`)
            .style("fill", "red")
            .append("svg:title")
            .text("Bar is truncated")
            .exit()
            // label bars
            .data(counts)
            .enter()
            .append("g")
            .append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", (c, i: number) => (i + .5) * this.barWidth)
            .attr("y", (d: number) => (!this.normalized && ((d * scale) < this.getChartHeight())) ?
                this.getChartHeight() - (d * scale) : 0)
            .attr("text-anchor", "middle")
            .attr("dy", (d: number) => this.normalized ? 0 : d <= (9 * displayMax / 10) ? "-.25em" : ".75em")
            .text((d: number) => Plot.boxHeight(d, this.samplingRate, this.getDisplayedPoints()))
            .exit();

        let noX = 0;
        if (this.heatmap.histogramMissingX != null) {
            for (const bucket of this.heatmap.histogramMissingX.buckets)
                noX += bucket;
        }

        if (displayMax <= 0) {
            this.plottingSurface.reportError("All values are missing: " + noX + " have no X value, "
                + this.heatmap.missingData + " have no X or Y value");
            return;
        }

        this.drawAxes();
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

    /**
     * The total count of missing values displayed as rectangles.
     */
    public getMissingDisplayed(): number {
        return this.missingDisplayed;
    }

    /**
     * The total count of points that correspond to displayed rectangles.
     */
    public getDisplayedPoints(): number {
        return this.visiblePoints + this.missingDisplayed;
    }

    protected rectHeight(d: Box, counts: number[], scale: number): number {
        if (this.normalized) {
            const c = counts[d.xIndex];
            if (c <= 0)
                return 0;
            return this.getChartHeight() * d.count / c;
        }
        return d.count * scale;
    }

    protected rectPosition(d: Box, counts: number[], scale: number): number {
        const y = d.countBelow + d.count;
        if (this.normalized) {
            const c = counts[d.xIndex];
            if (c <= 0)
                return 0;
            return this.getChartHeight() * (1 - y / c);
        }
        return this.getChartHeight() - y * scale;
    }

    /**
     * Get the information about the box at the specified coordinates.
     * @param x  This is an X position as a pixel in the chart coordinates.
     * @param yScaled  This is a scaled y value on the Y axis.
     */
    public getBoxInfo(x: number, yScaled: number): Box | null {
        let scale = 1.0;
        // Find out the rectangle where the mouse is
        const xIndex = Math.floor(x / this.getBarWidth());
        let perc: number = 0;
        let colorIndex: number = null;
        let found = false;
        if (xIndex < 0 || xIndex >= this.heatmap.buckets.length)
            return null;

        const values: number[] = this.heatmap.buckets[xIndex];

        let total = 0;
        for (const v of values)
            total += v;
        if (this.heatmap.histogramMissingY != null)
            total += this.heatmap.histogramMissingY.buckets[xIndex];
        if (total <= 0)
            // There could be no data for this specific x value
            return null;

        if (this.normalized)
            scale = 100 / total;

        let yTotalScaled = 0;
        let yTotal = 0;
        for (let i = 0; i < values.length; i++) {
            yTotalScaled += values[i] * scale;
            yTotal += values[i];
            if (yTotalScaled >= yScaled && !found) {
                perc = values[i];
                colorIndex = i;
                found = true;
            }
        }

        if (this.heatmap.histogramMissingY != null) {
            const missing = this.heatmap.histogramMissingY.buckets[xIndex];
            yTotal += missing;
            yTotalScaled += missing * scale;
            if (!found && yTotalScaled >= yScaled) {
                perc = missing;
                colorIndex = -1;
            }
        }
        return {
            xIndex: xIndex,
            yIndex: colorIndex,
            count: perc,
            countBelow: yTotal
        };
    }

    // noinspection JSMethodCanBeStatic
    public color(d: number, max: number): string {
        if (d > max)
        // This is for the "missing" data
            return "none";
        if (max === 0)
            return Plot.colorMap(0);
        return Plot.colorMap(d / max);
    }
}

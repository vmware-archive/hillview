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
import {AxisDescription} from "../dataViews/axisData";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {D3Scale} from "./ui";
import {symbol, symbolTriangle} from "d3-shape";
import {Histogram2DBase} from "./histogram2DBase";

/**
 * Represents an SVG rectangle drawn on the screen.
 */
interface Box {
    /**
     * Bucket index on the X axis.  null if out of range.
     */
    xIndex: number | null;
    /**
     * Bucket index on the Y axis.  null if out of range.
     * Number of buckets for "missing" data.
     */
    yIndex: number | null;
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

/**
 * Draws a histogram with stacked bars.
 */
export class Histogram2DPlot extends Histogram2DBase {
    protected xPoints: number;
    protected yPoints: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public draw(): void {
        super.draw();
        this.xPoints = this.data.first.perBucket.length;
        this.yPoints = this.data.first.perBucket[0].perBucket.length;

        const counts: number[] = [];

        this.max = 0;
        const rects: Box[] = [];
        for (let x = 0; x < this.xPoints; x++) {
            let yTotal = 0;
            for (let y = 0; y < this.yPoints; y++) {
                const vis = this.data.first.perBucket[x].perBucket[y];
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
            const v = this.data.first.perBucket[x].perMissing;
            const rec: Box = {
                xIndex: x,
                countBelow: yTotal,
                yIndex: -1,
                count: v
            };
            rects.push(rec);
            yTotal += v;
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
            d3axisLeft<number>(this.yScale)
                .tickFormat(d3format(".2s")), 1, false, null);

        this.barWidth = this.getChartWidth() / this.xPoints;
        const scale = displayMax <= 0 ? 1 : this.getChartHeight() / displayMax;

        this.plottingSurface.getChart()
            .selectAll("g")
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", (d: Box) => d.xIndex! * this.barWidth)
            .attr("y", (d: Box) => this.rectPosition(d, counts, scale))
            .attr("height", (d: Box) => this.rectHeight(d, counts, scale))
            .attr("width", this.barWidth)
            .attr("fill", (d: Box) => this.color(d.yIndex!, this.yPoints - 1))
            .attr("stroke", "black")
            .attr("stroke-width", (d: Box) => d.yIndex! < 0 ? 1 : 0)
            // overflow signs if necessary
            .data(counts)
            .enter()
            .append("g")
            .append("svg:path")
            // I am not sure how triangle size is measured; this 7 below seems to work find
            .attr("d", symbol().type(symbolTriangle).size(
                (d: number) => (!this.normalized && ((d * scale) > this.getChartHeight())) ?
                    7 * this.barWidth : 0))
            .attr("transform", (c: Box, i: number) => `translate(${(i + .5) * this.barWidth}, 0)`)
            .style("fill", "red")
            .append("svg:title")
            .text("Bar is truncated")
            // label bars
            .data(counts)
            .enter()
            .append("g")
            .append("text")
            .attr("class", "histogramBoxLabel")
            .attr("x", (c: Box, i: number) => (i + .5) * this.barWidth)
            .attr("y", (d: number) => (!this.normalized && ((d * scale) < this.getChartHeight())) ?
                this.getChartHeight() - (d * scale) : 0)
            .attr("text-anchor", "middle")
            .attr("dy", (d: number) => this.normalized ? 0 : d <= (9 * displayMax / 10) ? "-.25em" : ".75em")
            .text((d: number) => Plot.boxHeight(d, this.samplingRate, this.rowCount));

        if (displayMax <= 0) {
            this.plottingSurface.reportError("All values are missing.");
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

    protected rectHeight(d: Box, counts: number[], scale: number): number {
        if (this.normalized) {
            const c = counts[d.xIndex!];
            if (c <= 0)
                return 0;
            return this.getChartHeight() * d.count / c;
        }
        return d.count * scale;
    }

    protected rectPosition(d: Box, counts: number[], scale: number): number {
        const y = d.countBelow + d.count;
        if (this.normalized) {
            const c = counts[d.xIndex!];
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
        let yIndex: number | null = null;
        let found = false;
        if (xIndex < 0 || xIndex >= this.xPoints)
            return null;

        const values: number[] = this.data.first.perBucket[xIndex].perBucket;

        let total = 0;
        for (const v of values)
            total += v;
        total += this.data.first.perBucket[xIndex].perMissing;
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
                yIndex = i;
                found = true;
            }
        }

        const missing = this.data.first.perBucket[xIndex].perMissing;
        yTotal += missing;
        yTotalScaled += missing * scale;
        if (!found && yTotalScaled >= yScaled) {
            perc = missing;
            yIndex = values.length;  // missing
        }
        return {
            xIndex: xIndex,
            yIndex: yIndex,
            count: perc,
            countBelow: yTotal
        };
    }
}

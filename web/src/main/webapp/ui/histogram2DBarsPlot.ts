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
import {AxisDescription, NoBucketIndex} from "../dataViews/axisData";
import {PlottingSurface} from "./plottingSurface";
import {D3Scale} from "./ui";
import {Histogram2DBase} from "./histogram2DBase";
import {add} from "../util";

/**
 * Represents an SVG rectangle drawn on the screen.
 */
interface Box {
    xCoordinate: number | null;
    color: number;
    /**
     * Count of items represented by the rectangle.
     */
    count: number;
}

export interface BarInfo {
    colorIndex: number | null;
    count: number;
    bucketIndex: number | null;
}

/**
 * Draws the same data as a Histogram2DPlot but the bars are not stacked.
 */
export class Histogram2DBarsPlot extends Histogram2DBase {
    // The following are only set when drawing
    protected xPoints: number;
    protected yPoints: number;
    protected showMissing: boolean;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public draw(): void {
        super.draw();
        this.showMissing = false;
        this.xPoints = this.data.first.perBucket.length;
        this.yPoints = this.data.first.perBucket[0].perBucket.length;
        const missingYSum = this.data.first.perBucket.map(b => b.perMissing).reduce(add, 0);
        if (missingYSum > 0) {
            this.showMissing = true;
            this.yPoints++;
        }

        this.max = 0;
        const rects: Box[] = [];
        for (let x = 0; x < this.xPoints; x++) {
            let yTotal = 0;
            const xBuckets = this.data.first.perBucket[x];
            for (let y = 0; y < xBuckets.perBucket.length; y++) {
                const vis = xBuckets.perBucket[y];
                if (vis !== 0) {
                    const rect: Box = {
                        xCoordinate: x * (this.yPoints + 1) + y, // +1 for a space
                        color: y,
                        count: vis
                    };
                    rects.push(rect);
                    if (vis > this.max)
                        this.max = vis;
                }
                yTotal += vis;
            }
            const vis = this.data.first.perBucket[x].perMissing;
            const rec: Box = {
                xCoordinate: x * (this.yPoints + 1) + this.yPoints - 1,
                color: -1,
                count: vis
            };
            rects.push(rec);
            if (vis > this.max)
                this.max = vis;
        }
        /*
        TODO: show in a different plot
        if (this.heatmap.histogramMissingX != null) {
            xPoints++;
            for (let y = 0; y < yPoints; y++) {
                const vis = this.heatmap.histogramMissingY.buckets[y];
                const rec: Box = {
                    xCoordinate: (xPoints - 1) * y + yPoints - 1,
                    color: y,
                    count: vis
                };
                rects.push(rec);
                if (vis > this.max)
                    this.max = vis;
                this.missingDisplayed += vis;
            }
        }*/

        const displayMax = this.maxYAxis != null ? this.maxYAxis : this.max;
        this.yScale = d3scaleLinear()
            .range([this.getChartHeight(), 0]);
        this.yScale.domain([0, displayMax]);
        this.yAxis = new AxisDescription(
            d3axisLeft<number>(this.yScale)
                .tickFormat(d3format(".2s")), 1, false, null);

        const bucketCount = this.xPoints * (this.yPoints + 1); // + 1 for a space between groups
        this.barWidth = this.getChartWidth() / bucketCount;
        const scale = displayMax <= 0 ? 1 : this.getChartHeight() / displayMax;

        this.plottingSurface.getChart()
            .selectAll("g")
            .data(rects)
            .enter().append("g")
            .append("svg:rect")
            .attr("x", (d: Box) => d.xCoordinate! * this.barWidth)
            .attr("y", (d: Box) => this.getChartHeight() - this.rectHeight(d, scale))
            .attr("height", (d: Box) => this.rectHeight(d, scale))
            .attr("width", this.barWidth - 1)
            .attr("fill", (d: Box) => this.color(d.color, this.data.first.perBucket[0].perBucket.length - 1))
            .attr("stroke", "black")
            .attr("stroke-width", (d: Box) => d.color < 0 ? 1 : 0)
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

    protected rectHeight(d: Box, scale: number): number {
        return d.count * scale;
    }

    public getBarInfo(mouseX: number, dy: number): BarInfo {
        const bucketWidth = this.getChartWidth() / this.xPoints;
        let bucketIndex: number | null = Math.floor(mouseX / bucketWidth);
        const withinBucketOffset = mouseX - bucketIndex * bucketWidth;
        let colorIndex: number | null = Math.floor(withinBucketOffset / this.barWidth);
        let count = 0;

        if (colorIndex == this.yPoints) {
            colorIndex = NoBucketIndex;
        }

        if (bucketIndex >= 0 && bucketIndex < this.data.first.perBucket.length) {
            const bucket = this.data.first.perBucket[bucketIndex];
            if (this.showMissing && colorIndex == this.yPoints - 1)
                count = bucket.perMissing;
            else
                count = bucket.perBucket[colorIndex!];
        } else {
            bucketIndex = NoBucketIndex;
            colorIndex = NoBucketIndex;
        }
        return {
            colorIndex: colorIndex, // This could be null for the space between buckets
            bucketIndex: bucketIndex,
            count: count
        };
    }
}

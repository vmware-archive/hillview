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

import {line as d3line} from "d3-shape";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";

export interface ICDFPlot {
    setData(cdf: number[], adjust: boolean): void;
    draw(): void;
    getY(x: number): number;
}

/**
 * A CDFPlot draws a CDF curve on a PlottingSurface.
 */
export class CDFPlot extends Plot implements ICDFPlot {
    protected cdf: number[];
    // Data displayed on the screen, not exactly the same as cdf
    protected cdfData: number[];
    protected max: number;
    // True if we need to adjust for the range of the data
    // for discrete data.  When data is discrete the range contains
    // two extra half-intervals.
    protected adjust: boolean;
    protected bucketWidth: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public setData(cdf: number[], adjust: boolean): void {
        this.cdf = cdf;
        this.adjust = adjust;
        this.cdfData = [];
        let point = 0;
        if (adjust) {
            this.cdfData.push(point);
        }
        for (const bucket of cdf) {
            // each point is inserted twice.
            this.cdfData.push(point);
            point = bucket;
            this.cdfData.push(point);
        }
        this.max = point;
        if (adjust) {
            this.cdfData.push(point);
        }
        if (this.max === 0)
            // To prevent division by zero below.  It won't matter anyway
            this.max = 1;
    }

    public draw(): void {
        if (this.cdfData.length === 0)
            return;
        // After resizing the line may not have the exact number of points
        // as the screen width.
        const chartWidth = this.getChartWidth();
        const chartHeight = this.getChartHeight();
        this.bucketWidth = this.adjust ?
            2 * chartWidth / (this.cdfData.length - 2) :
            2 * chartWidth / this.cdfData.length;
        const cdfLine = d3line<number>()
            .x((d, i) => {
                if (this.adjust) {
                    const index = Math.floor((i + 1) / 2);
                    let x = index * this.bucketWidth;
                    // If adjusting the first and last buckets are half-width
                    if (index > 0)
                        x -= this.bucketWidth / 2;
                    if (i === this.cdfData.length - 1)
                        x -= this.bucketWidth / 2;
                    return x;
                } else {
                    // two points for each data point, for a zig-zag
                    const index = Math.floor(i / 2);
                    return index * this.bucketWidth;
                }
            })
            .y((d) => chartHeight - d * chartHeight / this.max);

        // draw CDF curve
        this.plottingSurface.getChart()
            .append("path")
            .datum(this.cdfData)
            .attr("stroke", "blue")
            .attr("d", cdfLine)
            .attr("fill", "none");
    }

    /**
     * Given an X coordinate find the corresponding Y in %
     * on the CDF curve.
     */
    public getY(x: number): number {
        // determine mouse position on cdf curve
        if (this.cdf == null)
            return 0;
        if (this.adjust)
            x += this.bucketWidth / 2;
        const cdfX = x / this.bucketWidth;
        if (cdfX < 0) {
            return 0;
        } else if (cdfX >= this.cdf.length) {
            return 1;
        } else {
            // 2 values for each pixel
            const cdfPosition = this.cdfData[2 * Math.floor(cdfX)];
            return cdfPosition / this.max;
        }
    }
}

/**
 * Can substitute for a CDFPlot, but does nothing.
 */
export class NoCDFPlot implements ICDFPlot {
    public setData(cdf: number[], adjust: boolean): void {}
    public draw(): void {}
    public getY(x: number): number { return 0; }
}
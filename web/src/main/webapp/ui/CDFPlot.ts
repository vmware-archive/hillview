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

import { d3 } from "./d3-modules";
import {Plot} from "./plot";
import {PlottingSurface} from "./plottingSurface";
import {Histogram} from "../javaBridge";

/**
 * A CDFPlot draws a CDF curve on a PlottingSurface.
 */
export class CDFPlot extends Plot {
    protected cdf: Histogram;
    protected cdfData: number[];
    protected max: number;

    public constructor(protected plottingSurface: PlottingSurface) {
        super(plottingSurface);
    }

    public setData(cdf: Histogram): void {
        this.cdf = cdf;
        this.cdfData = [];
        let point = 0;
        for (let i in cdf.buckets) {
            // yes, each point is inserted twice.
            this.cdfData.push(point);
            point += cdf.buckets[i];
            this.cdfData.push(point);
        }
        this.max = point;
    }

    public draw(): void {
        // After resizing the line may not have the exact number of points
        // as the screen width.
        let chartWidth = this.getChartWidth();
        let chartHeight = this.getChartHeight();
        let cdfLine = d3.line<number>()
            .x((d, i) => {
                let index = Math.floor(i / 2); // two points for each data point, for a zig-zag
                return index * 2 * chartWidth / this.cdfData.length;
            })
            .y(d => chartHeight - d * chartHeight / this.max);

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
        let cdfX = x * this.cdf.buckets.length / this.getChartWidth();
        if (cdfX < 0) {
            return 0;
        } else if (cdfX >= this.cdf.buckets.length) {
            return 1;
        } else {
            // 2 values for each pixel
            let cdfPosition = this.cdfData[2 * Math.floor(cdfX)];
            return cdfPosition / this.max;
        }
    }
}

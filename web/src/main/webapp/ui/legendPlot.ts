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
import {Rectangle, Resolution} from "./ui";
import {AxisData} from "../dataViews/axisData";
import {Histogram2DView} from "../dataViews/histogram2DView";

/**
 * Displays a legend for a 2D histogram.
 */
export class LegendPlot extends Plot {
    protected axisData: AxisData;
    protected legendRect: Rectangle;
    protected missingLegend: boolean;  // if true display legend for missing

    public constructor(surface) {
        super(surface);
    }

    public draw(): void {
        this.plottingSurface.getCanvas()
            .append("text")
            .text(this.axisData.description.name)
            .attr("transform", `translate(${this.getChartWidth() / 2}, 0)`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");

        let width = Resolution.legendBarWidth;
        if (width > this.getChartWidth())
            width = this.getChartWidth();
        let height = 15;

        let x = (this.getChartWidth() - width) / 2;
        let y = Resolution.legendSpaceHeight / 3;
        this.legendRect = new Rectangle({ x: x, y: y }, { width: width, height: height });
        let canvas = this.plottingSurface.getCanvas();

        let colorWidth = width / this.axisData.bucketCount;
        for (let i = 0; i < this.axisData.bucketCount; i++) {
            let color = Histogram2DView.colorMap(i / this.axisData.bucketCount);
            canvas.append("rect")
                .attr("width", colorWidth)
                .attr("height", height)
                .style("fill", color)
                .attr("x", x)
                .attr("y", y);
            x += colorWidth;
        }

        let scaleAxis = this.axisData.scaleAndAxis(this.legendRect.width(), true, true);
        // create a scale and axis for the legend
        this.xScale = scaleAxis.scale;
        this.xAxis = scaleAxis.axis;
        canvas.append("g")
            .attr("transform", `translate(${this.legendRect.lowerLeft().x},
                                              ${this.legendRect.lowerLeft().y})`)
            .call(this.xAxis);

        if (this.missingLegend) {
            let missingGap = 30;
            let missingWidth = 20;
            let missingHeight = 15;
            let missingX = 0;
            let missingY = 0;
            if (this.legendRect != null) {
                missingX = this.legendRect.upperRight().x + missingGap;
                missingY = this.legendRect.upperRight().y;
            } else {
                missingX = this.getChartWidth() / 2;
                missingY = Resolution.legendSpaceHeight / 3;
            }

            canvas.append("rect")
                .attr("width", missingWidth)
                .attr("height", missingHeight)
                .attr("x", missingX)
                .attr("y", missingY)
                .attr("stroke", "black")
                .attr("fill", "none")
                .attr("stroke-width", 1);

            canvas.append("text")
                .text("missing")
                .attr("transform", `translate(${missingX + missingWidth / 2}, ${missingY + missingHeight + 7})`)
                .attr("text-anchor", "middle")
                .attr("font-size", 10)
                .attr("dominant-baseline", "text-before-edge");
        }
    }

    setData(axis: AxisData, missingLegend: boolean): void {
        this.axisData = axis;
        this.missingLegend = missingLegend;
    }

    public legendRectangle(): Rectangle {
        return this.legendRect;
    }
}

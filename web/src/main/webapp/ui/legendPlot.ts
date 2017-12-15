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

        // apparently SVG defs are global, even if they are in
        // different SVG elements.  So we have to assign unique names.
        let gradientId = 'gradient' + this.plottingSurface.page.pageId;
        let gradient = this.plottingSurface.getCanvas()
            .append('defs')
            .append('linearGradient')
            .attr('id', gradientId)
            .attr('x1', '0%')
            .attr('y1', '0%')
            .attr('x2', '100%')
            .attr('y2', '0%')
            .attr('spreadMethod', 'pad');

        for (let i = 0; i <= 100; i += 4) {
            gradient.append("stop")
                .attr("offset", i + "%")
                .attr("stop-color", Histogram2DView.colorMap(i / 100))
                .attr("stop-opacity", 1)
        }

        let width = Resolution.legendSize.width;
        if (width > this.getChartWidth())
            width = this.getChartWidth();
        let height = 15;

        let x = (this.getChartWidth() - width) / 2;
        let y = Resolution.legendSize.height / 3;
        this.legendRect = new Rectangle({ x: x, y: y }, { width: width, height: height });

        let canvas = this.plottingSurface.getCanvas();
        canvas.append("rect")
            .attr("width", this.legendRect.width())
            .attr("height", this.legendRect.height())
            .style("fill", "url(#" + gradientId + ")")
            .attr("x", this.legendRect.upperLeft().x)
            .attr("y", this.legendRect.upperLeft().y);

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
                missingY = Resolution.legendSize.height / 3;
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

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

import {drag as d3drag} from "d3-drag";
import {mouse as d3mouse} from "d3-selection";
import {AxisData, AxisKind} from "../dataViews/axisData";
import {Plot} from "./plot";
import {D3SvgElement, Point, Rectangle, Resolution} from "./ui";
import {SchemaClass} from "../schemaClass";

/**
 * Displays a legend for a 2D histogram.
 */
export class HistogramLegendPlot extends Plot {
    protected axisData: AxisData;
    protected legendRect: Rectangle;
    protected missingLegend: boolean;  // if true display legend for missing
    protected hilightRect: D3SvgElement;
    protected missingX: number;
    protected missingY: number;
    protected readonly missingGap = 30;
    protected readonly missingWidth = 20;
    protected readonly height = 15;
    protected colorWidth: number;
    protected x: number;
    protected y: number;
    protected width: number;
    protected schema: SchemaClass;
    protected drawn: boolean;
    protected dragging: boolean;
    protected moved: boolean;
    protected legendSelectionRectangle: D3SvgElement;
    protected selectionCompleted: (xl: number, xr: number) => void;
    /**
     * Coordinates of mouse within canvas.
     */
    protected selectionOrigin: Point;

    public constructor(surface, onCompleted: (xl: number, xr: number) => void) {
        super(surface);
        this.drawn = false;
        this.selectionCompleted = onCompleted;
        this.dragging = false;
        this.moved = false;
    }

    // dragging in the legend
    protected dragLegendStart(): void {
        this.dragging = true;
        this.moved = false;
        const position = d3mouse(this.plottingSurface.getCanvas().node());
        this.selectionOrigin = {
            x: position[0],
            y: position[1] };
    }

    protected dragLegendMove(): void {
        if (!this.dragging || !this.drawn)
            return;
        this.moved = true;
        let ox = this.selectionOrigin.x;
        const position = d3mouse(this.plottingSurface.getCanvas().node());
        const x = position[0];
        let width = x - ox;
        const height = this.legendRect.height();

        if (width < 0) {
            ox = x;
            width = -width;
        }
        this.legendSelectionRectangle
            .attr("x", ox)
            .attr("width", width)
            .attr("y", this.legendRect.upperLeft().y)
            .attr("height", height);

        // Prevent the selection from spilling out of the legend itself
        if (ox < this.legendRect.origin.x) {
            const delta = this.legendRect.origin.x - ox;
            this.legendSelectionRectangle
                .attr("x", this.legendRect.origin.x)
                .attr("width", width - delta);
        } else if (ox + width > this.legendRect.lowerRight().x) {
            const delta = ox + width - this.legendRect.lowerRight().x;
            this.legendSelectionRectangle
                .attr("width", width - delta);
        }
    }

    protected dragLegendEnd(): void {
        if (!this.dragging || !this.moved || !this.drawn)
            return;
        this.dragging = false;
        this.moved = false;
        this.legendSelectionRectangle
            .attr("width", 0)
            .attr("height", 0);

        const position = d3mouse(this.plottingSurface.getCanvas().node());
        const x = position[0];
        if (this.selectionCompleted != null)
            this.selectionCompleted(this.selectionOrigin.x - this.legendRect.lowerLeft().x,
                x - this.legendRect.lowerLeft().x);
    }

    public draw(): void {
        this.plottingSurface.getCanvas()
            .append("text")
            .text(this.axisData.getDisplayNameString(this.schema))
            .attr("transform", `translate(${this.getChartWidth() / 2}, 0)`)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "text-before-edge");
        const legendDrag = d3drag()
            .on("start", () => this.dragLegendStart())
            .on("drag", () => this.dragLegendMove())
            .on("end", () => this.dragLegendEnd());
        const canvas = this.plottingSurface.getCanvas();
        canvas.call(legendDrag);

        this.width = Resolution.legendBarWidth;
        if (this.width > this.getChartWidth())
            this.width = this.getChartWidth();

        this.x = (this.getChartWidth() - this.width) / 2;
        this.y = Resolution.legendSpaceHeight / 3;
        let x = this.x;
        this.legendRect = new Rectangle({ x: this.x, y: this.y }, { width: this.width, height: this.height });

        this.colorWidth = this.width / this.axisData.bucketCount;
        for (let i = 0; i < this.axisData.bucketCount; i++) {
            let color: string;
            if (this.axisData.bucketCount === 1)
                color = Plot.colorMap(0);
            else
                color = Plot.colorMap(i / (this.axisData.bucketCount - 1));
            canvas.append("rect")
                .attr("width", this.colorWidth)
                .attr("height", this.height)
                .style("fill", color)
                .attr("x", x)
                .attr("y", this.y)
                .append("title")
                .text(this.axisData.bucketDescription(i, 100));
            x += this.colorWidth;
        }

        this.axisData.setResolution(this.legendRect.width(), AxisKind.Legend, Resolution.legendSpaceHeight / 3);
        const g = canvas.append("g")
            .attr("transform", `translate(${this.legendRect.lowerLeft().x},
                                          ${this.legendRect.lowerLeft().y})`)
            .attr("class", "x-axis");
        this.axisData.axis.draw(g);

        if (this.missingLegend) {
            if (this.legendRect != null) {
                this.missingX = this.legendRect.upperRight().x + this.missingGap;
                this.missingY = this.legendRect.upperRight().y;
            } else {
                this.missingX = this.getChartWidth() / 2;
                this.missingY = Resolution.legendSpaceHeight / 3;
            }

            canvas.append("rect")
                .attr("width", this.missingWidth)
                .attr("height", this.height)
                .attr("x", this.missingX)
                .attr("y", this.missingY)
                .attr("stroke", "black")
                .attr("fill", "none")
                .attr("stroke-width", 1);

            canvas.append("text")
                .text("missing")
                .attr("transform", `translate(${this.missingX + this.missingWidth / 2},
                                              ${this.missingY + this.height + 7})`)
                .attr("text-anchor", "middle")
                .attr("font-size", 10)
                .attr("dominant-baseline", "text-before-edge");
        }

        this.hilightRect = canvas.append("rect")
            .attr("class", "dashed")
            .attr("height", this.height)
            .attr("x", 0)
            .attr("y", 0)
            .attr("stroke-dasharray", "5,5")
            .attr("stroke", "cyan")
            .attr("fill", "none");
        this.drawn = true;

        this.legendSelectionRectangle = canvas
            .append("rect")
            .attr("class", "dashed")
            .attr("width", 0)
            .attr("height", 0);
    }

    /**
     * Highlight the color with the specified index.  Special values:
     * - colorIndex is < 0: missing box
     * - colorIndex is null: nothing
     */
    public highlight(colorIndex: number | null): void {
        if (colorIndex == null) {
            this.hilightRect
                .attr("width", 0);
        } else if (colorIndex < 0) {
            this.hilightRect
                .attr("x", this.missingX)
                .attr("y", this.missingY)
                .attr("width", this.missingWidth);
        } else {
            this.hilightRect
                .attr("width", this.colorWidth)
                .attr("x", this.x + colorIndex * this.colorWidth)
                .attr("y", this.y);
        }
    }

    public setData(axis: AxisData, missingLegend: boolean, schema: SchemaClass): void {
        this.axisData = axis;
        this.missingLegend = missingLegend;
        this.schema = schema;
    }

    public legendRectangle(): Rectangle {
        return this.legendRect;
    }
}


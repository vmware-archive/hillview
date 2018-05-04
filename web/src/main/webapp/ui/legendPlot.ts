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
import {ContextMenu} from "./menu";
import {PlottingSurface} from "./plottingSurface";
import {interpolateWarm as d3interpolateWarm,
    interpolateCool as d3interpolateCool} from "d3-scale-chromatic";
import {scaleLog as d3scaleLog, scaleLinear as d3scaleLinear} from "d3-scale";
import {axisBottom as d3axisBottom} from "d3-axis";
import {event as d3event} from "d3-selection";

/**
 * Displays a legend for a 2D histogram.
 */
export class HistogramLegendPlot extends Plot {
    protected axisData: AxisData;
    protected legendRect: Rectangle;
    protected missingLegend: boolean;  // if true display legend for missing
    protected hilightRect: any;
    protected missingX: number;
    protected missingY: number;
    protected readonly missingGap = 30;
    protected readonly missingWidth = 20;
    protected readonly height = 15;
    protected colorWidth: number;
    protected x: number;
    protected y: number;
    protected width: number;

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

        this.width = Resolution.legendBarWidth;
        if (this.width > this.getChartWidth())
            this.width = this.getChartWidth();

        this.x = (this.getChartWidth() - this.width) / 2;
        this.y = Resolution.legendSpaceHeight / 3;
        let x = this.x;
        this.legendRect = new Rectangle({ x: this.x, y: this.y }, { width: this.width, height: this.height });
        let canvas = this.plottingSurface.getCanvas();

        this.colorWidth = this.width / this.axisData.bucketCount;
        for (let i = 0; i < this.axisData.bucketCount; i++) {
            let color: string;
            if (this.axisData.bucketCount == 1)
                color = Histogram2DView.colorMap(0);
            else
                color = Histogram2DView.colorMap(i / (this.axisData.bucketCount - 1));
            canvas.append("rect")
                .attr("width", this.colorWidth)
                .attr("height", this.height)
                .style("fill", color)
                .attr("x", x)
                .attr("y", this.y);
            x += this.colorWidth;
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
    }

    /**
     * Hilight the color with the specified index.  Special values:
     * - colorIndex is < 0: missing box
     * - colorIndex is null: nothing
     */
    hilight(colorIndex: number): void {
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

    setData(axis: AxisData, missingLegend: boolean): void {
        this.axisData = axis;
        this.missingLegend = missingLegend;
    }

    public legendRectangle(): Rectangle {
        return this.legendRect;
    }
}

/**
 * Represents a map from the range 0-1 to colors.
 */
class HeatmapColormap {
    /**
     *  Suggested threshold for when a log-scale should be used.
     */
    public static logThreshold = 50;
    public logScale: boolean;

    protected map: (x) => string;

    constructor(public readonly min: number, public readonly max: number) {
        this.setMap(d3interpolateWarm);
    }

    public setLogScale(logScale: boolean) {
        this.logScale = logScale;
    }

    public setMap(map: (number) => string): void {
        this.map = map;
    }

    public valueMap(x: number): string {
        return this.map(x);
    }

    public apply(x: number): string {
        if (this.logScale)
            return this.applyLog(x);
        else
            return this.applyLinear(x);
    }

    private applyLinear(x: number): string {
        return this.map(x / this.max);
    }

    private applyLog(x: number): string {
        return this.map(Math.log(x) / Math.log(this.max));
    }
}

/**
 * Displays a color map suitable for heatmaps.
 */
export class HeatmapLegendPlot extends Plot {
    /* Static counter that increments to assign every ColorLegend object
       a unique ID for the gradient element. */
    private static nextUniqueId: number = 0;
    private uniqueId: number;
    private gradient: any; // Element that contains the definitions for the colors in the color map

    private onColorMapChange: (ColorMap) => void; // Function that is called to update other elements when the color map changes.
    private contextMenu: ContextMenu;
    private barHeight: number;
    private colorMap: HeatmapColormap;
    private legendRectangle: any;
    private axisElement: any;

    constructor(surface: PlottingSurface) {
        super(surface);
        this.barHeight = 16;
        this.uniqueId = HeatmapLegendPlot.nextUniqueId++;
    }

    public setColorMapChangeEventListener(listener: (ColorMap) => void) {
        this.onColorMapChange = listener;
    }

    public clear(): void {
        super.clear();
        this.colorMap = null;
    }

    /**
     * The context menu is added only when a colormap change event listener is set.
     */
    private enableContextMenu() {
        this.contextMenu = new ContextMenu(
            this.plottingSurface.getHTMLRepresentation(), [
            {
                text: "Cool",
                help: "Use a color palette with cool colors.",
                action: () => {
                    this.colorMap.setMap(d3interpolateCool);
                    this.mapUpdated();
                }
            }, {
                text: "Warm",
                help: "Use a color palette with warm colors.",
                action: () => {
                    this.colorMap.setMap(d3interpolateWarm);
                    this.mapUpdated();
                }
            }, {
                text: "Gray",
                help: "Use a grayscale color palette.",
                action: () => {
                    this.colorMap.setMap((x: number) => `rgb(${Math.round(255 * (1 - x))}, 
                    ${Math.round(255 * (1 - x))}, ${Math.round(255 * (1 - x))})`);
                    this.mapUpdated();
                }
            }, {
                text: "Toggle log scale",
                help: "Switch between a linear and logarithmic scale on the colormap.",
                action: () => {
                    this.colorMap.setLogScale(!this.colorMap.logScale);
                    this.mapUpdated();
                }
            },
        ]);
    }

    private showContextMenu(event: MouseEvent) {
        /* Only show context menu if it is enabled. */
        if (this.contextMenu != null) {
            event.preventDefault();
            this.contextMenu.show(event);
        }
    }

    // Redraw the legend, and notify the listeners.
    private mapUpdated() {
        this.setData(this.colorMap.min, this.colorMap.max, this.colorMap.logScale);
        this.draw();
        // Notify the onColorChange listener (redraw the elements with new colors)
        if (this.onColorMapChange != null)
            this.onColorMapChange(this.colorMap);
    }

    setData(min: number, max: number, useLogScale?: boolean): void {
        let base = (max - min) > 10000 ? 10 : 2;
        if (this.colorMap == null)
            this.colorMap = new HeatmapColormap(min, max);
        console.assert(min == this.colorMap.min);
        console.assert(max == this.colorMap.max);
        let logScale = useLogScale != null ? useLogScale : max > HeatmapColormap.logThreshold;
        this.colorMap.setLogScale(logScale);
        if (logScale) {
            this.xScale = d3scaleLog().base(base);
        } else
            this.xScale = d3scaleLinear();
        this.xScale
            .domain([min, max])
            .range([0, Resolution.legendBarWidth]);
        let ticks = Math.min(max, 10);
        this.xAxis = d3axisBottom(this.xScale).ticks(ticks);
    }

    public getColor(v: number): any {
        return this.colorMap.apply(v);
    }

    public draw(): void {
        if (this.contextMenu == null)
            this.enableContextMenu();
        if (this.legendRectangle != null) {
            this.legendRectangle.remove();
            this.axisElement.remove();
            if (this.gradient != null)
                this.gradient.remove();
            this.gradient = null;
        }
        let min = this.colorMap.min;
        let max = this.colorMap.max;
        let canvas = this.plottingSurface.getCanvas();
        let x = (this.plottingSurface.getActualChartWidth() - Resolution.legendBarWidth) / 2;

        if (!this.colorMap.logScale && this.colorMap.max - this.colorMap.min < 25) {
            // use discrete colors
            let colorCount = max - min + 1;
            let colorWidth = Resolution.legendBarWidth / colorCount;
            let rectX = x;
            for (let i = this.colorMap.min; i <= this.colorMap.max; i++) {
                let color = this.getColor(i);
                canvas.append("rect")
                    .attr("width", colorWidth)
                    .attr("height", this.barHeight)
                    .style("fill", color)
                    .attr("x", rectX)
                    .attr("y", 0);
                rectX += colorWidth;
            }
        } else {
            this.gradient = canvas.append('defs')
                .append('linearGradient')
                .attr('id', `gradient${this.uniqueId}`)
                .attr('x1', '0%')
                .attr('y1', '0%')
                .attr('x2', '100%')
                .attr('y2', '0%')
                .attr('spreadMethod', 'pad');
            for (let i = 0; i <= 100; i += 4)
                this.gradient.append("stop")
                    .attr("offset", i + "%")
                    .attr("stop-color", this.colorMap.valueMap(i / 100))
                    .attr("stop-opacity", 1)
        }
        this.legendRectangle = canvas.append("rect")
            .attr("x", x)
            .attr("y", 0)
            .attr("width", Resolution.legendBarWidth)
            .attr("height", this.barHeight)
            .style("fill", this.gradient != null ? `url(#gradient${this.uniqueId})` : "none");
        this.legendRectangle.on("contextmenu", () => this.showContextMenu(d3event));
        this.axisElement = canvas.append("g")
            .attr("transform", `translate(${x}, ${this.barHeight})`)
            .call(this.xAxis);
    }
}


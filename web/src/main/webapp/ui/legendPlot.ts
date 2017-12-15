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
import {d3} from "./d3-modules";
import {PlottingSurface} from "./plottingSurface";

/**
 * Displays a legend for a 2D histogram.
 */
export class HistogramLegendPlot extends Plot {
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
        this.setMap(d3.interpolateWarm);
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
                    this.colorMap.setMap(d3.interpolateCool);
                    this.mapUpdated();
                }
            }, {
                text: "Warm",
                help: "Use a color palette with warm colors.",
                action: () => {
                    this.colorMap.setMap(d3.interpolateWarm);
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
            this.xScale = d3.scaleLog().base(base);
        } else
            this.xScale = d3.scaleLinear();
        this.xScale
            .domain([min, max])
            .range([0, Resolution.legendBarWidth]);
        let ticks = Math.min(max, 10);
        this.xAxis = d3.axisBottom(this.xScale).ticks(ticks);
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
        this.legendRectangle.on("contextmenu", () => this.showContextMenu(d3.event));
        this.axisElement = canvas.append("g")
            .attr("transform", `translate(${x}, ${this.barHeight})`)
            .call(this.xAxis);
    }
}


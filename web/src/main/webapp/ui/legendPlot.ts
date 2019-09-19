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

import {axisBottom as d3axisBottom} from "d3-axis";
import {drag as d3drag} from "d3-drag";
import {scaleLinear as d3scaleLinear, scaleLog as d3scaleLog} from "d3-scale";
import {
    interpolateCool as d3interpolateCool,
    interpolateWarm as d3interpolateWarm
} from "d3-scale-chromatic";
import {event as d3event, mouse as d3mouse} from "d3-selection";
import {AxisData, AxisDescription, AxisKind} from "../dataViews/axisData";
import {assert} from "../util";
import {ContextMenu} from "./menu";
import {Plot} from "./plot";
import {HtmlPlottingSurface, PlottingSurface} from "./plottingSurface";
import {D3Axis, D3Scale, D3SvgElement, Point, Rectangle, Resolution} from "./ui";
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

    public setLogScale(logScale: boolean): void {
        this.logScale = logScale;
    }

    public setMap(map: (n: number) => string): void {
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
    private readonly uniqueId: number;
    private gradient: D3SvgElement; // Element that contains the definitions for the colors in the color map

    // Function that is called to update other elements when the color map changes.
    private onColorMapChange: (ColorMap) => void;
    private contextMenu: ContextMenu;
    private barHeight: number;
    private colorMap: HeatmapColormap;
    private legendRectangle: D3SvgElement;
    private axisElement: D3SvgElement;
    protected xAxis: D3Axis;
    protected drawn: boolean;

    constructor(surface: HtmlPlottingSurface) {
        super(surface);
        this.barHeight = 16;
        this.uniqueId = HeatmapLegendPlot.nextUniqueId++;
        this.drawn = false;
    }

    public setColorMapChangeEventListener(listener: (ColorMap) => void): void {
        this.onColorMapChange = listener;
    }

    /**
     * The context menu is added only when a colormap change event listener is set.
     */
    private enableContextMenu(): void {
        this.contextMenu = new ContextMenu(
            // menus cannot be attached to SVG objects
            (this.plottingSurface as HtmlPlottingSurface).topLevel, [
            {
                text: "Cool",
                help: "Use a color palette with cool colors.",
                action: () => {
                    this.colorMap.setMap(d3interpolateCool);
                    this.mapUpdated();
                },
            }, {
                text: "Warm",
                help: "Use a color palette with warm colors.",
                action: () => {
                    this.colorMap.setMap(d3interpolateWarm);
                    this.mapUpdated();
                },
            }, {
                text: "Gray",
                help: "Use a grayscale color palette.",
                action: () => {
                    this.colorMap.setMap((x: number) => `rgb(${Math.round(255 * (1 - x))},
                    ${Math.round(255 * (1 - x))}, ${Math.round(255 * (1 - x))})`);
                    this.mapUpdated();
                },
            }, {
                text: "Toggle log scale",
                help: "Switch between a linear and logarithmic scale on the colormap.",
                action: () => {
                    this.colorMap.setLogScale(!this.colorMap.logScale);
                    this.mapUpdated();
                },
            },
        ]);
    }

    public setSurface(surface: PlottingSurface): void {
        this.plottingSurface = surface;
    }

    private showContextMenu(event: MouseEvent): void {
        /* Only show context menu if it is enabled. */
        if (this.contextMenu != null) {
            event.preventDefault();
            this.contextMenu.show(event);
        }
    }

    // Redraw the legend, and notify the listeners.
    private mapUpdated(): void {
        this.setData(this.colorMap.min, this.colorMap.max, this.colorMap.logScale);
        this.draw();
        // Notify the onColorChange listener (redraw the elements with new colors)
        if (this.onColorMapChange != null)
            this.onColorMapChange(this.colorMap);
    }

    public setData(min: number, max: number, useLogScale?: boolean): void {
        const base = (max - min) > 10000 ? 10 : 2;
        if (this.colorMap == null)
            this.colorMap = new HeatmapColormap(min, max);
        assert(min === this.colorMap.min);
        assert(max === this.colorMap.max);
        const logScale = useLogScale != null ? useLogScale : max > HeatmapColormap.logThreshold;
        this.colorMap.setLogScale(logScale);

        let scale: D3Scale;
        if (logScale) {
            scale = d3scaleLog().base(base);
        } else
            scale = d3scaleLinear();
        scale
            .domain([min, max])
            .range([0, Resolution.legendBarWidth]);
        const ticks = Math.min(max, 10);
        this.xAxis = d3axisBottom(scale).ticks(ticks);
        this.drawn = true;
    }

    public getXAxis(): AxisDescription {
        return new AxisDescription(this.xAxis, 1, false, null);
    }

    public getColor(v: number): string {
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
        const min = this.colorMap.min;
        const max = this.colorMap.max;
        const canvas = this.plottingSurface.getCanvas();
        const x = (this.plottingSurface.getChartWidth() - Resolution.legendBarWidth) / 2;

        if (!this.colorMap.logScale && this.colorMap.max - this.colorMap.min < 25) {
            // use discrete colors
            const colorCount = max - min + 1;
            const colorWidth = Resolution.legendBarWidth / colorCount;
            let rectX = x;
            for (let i = this.colorMap.min; i <= this.colorMap.max; i++) {
                const color = this.getColor(i);
                canvas.append("rect")
                    .attr("width", colorWidth)
                    .attr("height", this.barHeight)
                    .style("fill", color)
                    .attr("x", rectX)
                    .attr("y", 0);
                rectX += colorWidth;
            }
        } else {
            this.gradient = canvas.append("defs")
                .append("linearGradient")
                .attr("id", `gradient${this.uniqueId}`)
                .attr("x1", "0%")
                .attr("y1", "0%")
                .attr("x2", "100%")
                .attr("y2", "0%")
                .attr("spreadMethod", "pad");
            for (let i = 0; i <= 100; i += 4)
                this.gradient.append("stop")
                    .attr("offset", i + "%")
                    .attr("stop-color", this.colorMap.valueMap(i / 100))
                    .attr("stop-opacity", 1);
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
            .call(this.getXAxis().axis);
    }
}

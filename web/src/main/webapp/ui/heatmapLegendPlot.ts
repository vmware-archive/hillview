/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
    interpolateCool as d3interpolateCool,
    interpolateWarm as d3interpolateWarm
} from "d3-scale-chromatic";
import {D3Axis, D3Scale, D3SvgElement, Resolution} from "./ui";
import {ContextMenu} from "./menu";
import {HtmlPlottingSurface, PlottingSurface} from "./plottingSurface";
import {assert, assertNever, ColorMap, desaturateOutsideRange} from "../util";
import {scaleLinear as d3scaleLinear, scaleLog as d3scaleLog} from "d3-scale";
import {axisBottom as d3axisBottom} from "d3-axis";
import {AxisDescription} from "../dataViews/axisData";
import {event as d3event} from "d3-selection";
import {LegendPlot} from "./legendPlot";

/**
 * Represents a map from the range 1-max to colors.
 */
class HeatmapColormap {
    /**
     *  Suggested threshold for when a log-scale should be used.
     */
    public static logThreshold = 50;
    public logScale: boolean | null;  // if null this is decided based on data.
    protected originalMap: ColorMap;
    public map: ColorMap;

    constructor(public readonly max: number) {
        this.setMap(d3interpolateWarm);
    }

    public setLogScale(logScale: boolean): void {
        this.logScale = logScale;
    }

    public setMap(map: ColorMap): void {
        // console.log("Changing map to: " + map(0) + "-" + map(1));
        this.originalMap = map;
        this.map = map;
    }

    public apply(x: number): string {
        if (this.logScale)
            x = this.applyLog(x);
        else
            x = this.applyLinear(x);
        return this.map(x);
    }

    private applyLinear(x: number): number {
        return x / this.max;
    }

    private applyLog(x: number): number {
        if (x <= 1)
            return 0;
        return Math.log(x) / Math.log(this.max);
    }

    // Colors outside the specified range are de-saturated.
    public desaturateOutsideRange(x0: number, x1: number): void {
        this.map = desaturateOutsideRange(this.originalMap, x0, x1);
    }
}

export enum ColorMapKind {
    Cool,
    Warm,
    Grayscale
}

/**
 * Displays a color map suitable for heatmaps.
 */
export class HeatmapLegendPlot extends LegendPlot<number> {
    /* Static counter that increments to assign every ColorLegend object
       a unique ID for the gradient element. */
    private static nextUniqueId: number = 0;
    private readonly uniqueId: number;
    private gradient: D3SvgElement; // Element that contains the definitions for the colors in the color map

    // Function that is called to update other elements when the color map changes.
    private onColorMapChange: (c: ColorMap) => void;
    private contextMenu: ContextMenu;
    public colorMap: HeatmapColormap;
    private svgRectangle: D3SvgElement;
    private axisElement: D3SvgElement;
    protected xAxis: D3Axis;
    protected scale: D3Scale;

    constructor(surface: HtmlPlottingSurface, onSelectionCompleted: (xl: number, xr: number) => void) {
        super(surface, onSelectionCompleted);
        this.uniqueId = HeatmapLegendPlot.nextUniqueId++;
        this.y = 0;
        this.createRectangle();
    }

    public max(): number {
        return this.data;
    }

    public setColorMapChangeEventListener(listener: (c: ColorMap) => void): void {
        this.onColorMapChange = listener;
    }

    /**
     * Highlight the specified range.
     */
    public highlight(min: number, max: number): void {
        if (min == null) {
            this.hilightRect
                .attr("width", 0);
        } else {
            const minX = min < 1 ? 0 : this.scale(min);
            const maxX = max < 1 ? 0 : this.scale(max);
            const x = (this.plottingSurface.getChartWidth() - Resolution.legendBarWidth) / 2;
            this.hilightRect
                .attr("width", maxX - minX)
                .attr("x", x + minX);
        }
    }

    public setColorMapKind(kind: ColorMapKind): void {
        switch (kind) {
            case ColorMapKind.Cool:
                this.colorMap.setMap(d3interpolateCool);
                break;
            case ColorMapKind.Warm:
                this.colorMap.setMap(d3interpolateWarm);
                break;
            case ColorMapKind.Grayscale:
                this.colorMap.setMap((x: number) => `rgb(
                ${Math.round(255 * (1 - x))},${Math.round(255 * (1 - x))},${Math.round(255 * (1 - x))})`);
                break;
            default:
                assertNever(kind);
        }
    }

    private createContextMenu(): void {
        this.contextMenu = new ContextMenu(
            // menus cannot be attached to SVG objects
            (this.plottingSurface as HtmlPlottingSurface).topLevel, [
                {
                    text: "Cool",
                    help: "Use a color palette with cool colors.",
                    action: () => {
                        this.setColorMapKind(ColorMapKind.Cool);
                        this.mapUpdated();
                    },
                }, {
                    text: "Warm",
                    help: "Use a color palette with warm colors.",
                    action: () => {
                        this.setColorMapKind(ColorMapKind.Warm);
                        this.mapUpdated();
                    },
                }, {
                    text: "Gray",
                    help: "Use a grayscale color palette.",
                    action: () => {
                        this.setColorMapKind(ColorMapKind.Grayscale);
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
            this.contextMenu.showAtMouse(event);
        }
    }

    public emphasizeRange(x0: number, x1: number): void {
        if (this.onColorMapChange != null) {
            this.colorMap.desaturateOutsideRange(x0 / this.width, x1 / this.width);
            this.onColorMapChange(this.colorMap.map);
        }
    }

    // Redraw the legend, and notify the listeners.
    private mapUpdated(): void {
        this.computeAxis();
        this.draw();
        // Notify the onColorChange listener (redraw the elements with new colors)
        if (this.onColorMapChange != null)
            this.onColorMapChange(this.colorMap.map);
    }

    protected computeAxis(): void {
        const base = this.max() > 10000 ? 10 : 2;
        if (this.colorMap.logScale)
            this.scale = d3scaleLog().base(base);
        else
            this.scale = d3scaleLinear();
        this.scale
            .domain([1, this.max()])
            .range([0, Resolution.legendBarWidth]);
        const ticks = Math.min(this.max(), 10);
        this.xAxis = d3axisBottom(this.scale).ticks(ticks);
    }

    public setData(max: number): void {
        this.data = max;
        if (max < 1)
            return;
        console.assert(this.max() > 0);
        this.colorMap = new HeatmapColormap(this.max());
        assert(this.max() === this.colorMap.max);
        if (this.colorMap.logScale == null)
            this.colorMap.setLogScale(this.max() > HeatmapColormap.logThreshold);
        this.computeAxis();
    }

    public getXAxis(): AxisDescription {
        return new AxisDescription(this.xAxis, 1, false, null);
    }

    public getColor(v: number): string {
        return this.colorMap.apply(v);
    }

    public draw(): void {
        if (this.max() == null)
            return;
        this.drawn = true;
        if (this.contextMenu == null)
            this.createContextMenu();
        if (this.svgRectangle != null) {
            this.svgRectangle.remove();
            this.axisElement.remove();
            if (this.gradient != null)
                this.gradient.remove();
            this.gradient = null;
        }

        const canvas = this.plottingSurface.getCanvas();
        if (!this.colorMap.logScale && this.max() < 25) {
            // use discrete colors
            const colorCount = this.max();
            const colorWidth = Resolution.legendBarWidth / colorCount;
            let rectX = this.x;
            for (let i = 1; i <= this.max(); i++) {
                const color = this.getColor(i);
                canvas.append("rect")
                    .attr("width", colorWidth)
                    .attr("height", this.height)
                    .style("fill", color)
                    .attr("x", rectX)
                    .attr("y", this.y);
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
                    // Reach behind the back into the underlying map
                    .attr("stop-color", this.colorMap.map(i / 100))
                    .attr("stop-opacity", 1);
        }
        this.svgRectangle = canvas.append("rect")
            .attr("x", this.x)
            .attr("y", this.y)
            .attr("width", Resolution.legendBarWidth)
            .attr("height", this.height)
            .style("fill", this.gradient != null ? `url(#gradient${this.uniqueId})` : "none");
        this.svgRectangle.on("contextmenu", () => this.showContextMenu(d3event));
        this.axisElement = canvas.append("g")
            .attr("transform", `translate(${this.x}, ${this.height})`)
            .call(this.getXAxis().axis);
        super.draw();
    }

    invert(x: number): number {
        return this.scale.invert(x);
    }

    public getColorMap(): ColorMap {
        return (x) => this.colorMap.apply(x);
    }
}

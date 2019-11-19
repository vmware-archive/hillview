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
import {Plot} from "./plot";
import {D3Axis, D3Scale, D3SvgElement, Resolution} from "./ui";
import {ContextMenu} from "./menu";
import {HtmlPlottingSurface, PlottingSurface} from "./plottingSurface";
import {assert} from "../util";
import {scaleLinear as d3scaleLinear, scaleLog as d3scaleLog} from "d3-scale";
import {axisBottom as d3axisBottom} from "d3-axis";
import {AxisDescription} from "../dataViews/axisData";
import {event as d3event} from "d3-selection";

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

export enum ColorMapKind {
    Cool,
    Warm,
    Grayscale
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
    protected hilightRect: D3SvgElement;
    protected scale: D3Scale;

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
     * Highlight the specified range.
     */
    public highlight(min: number, max: number): void {
        if (min == null) {
            this.hilightRect
                .attr("width", 0);
        } else {
            const minX = min < 1 ? 0 : this.scale(min);
            const maxX = this.scale(max);
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
                this.colorMap.setMap((x: number) => `rgb(${Math.round(255 * (1 - x))},
                    ${Math.round(255 * (1 - x))}, ${Math.round(255 * (1 - x))})`);
                break;
        }
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

        if (logScale) {
            this.scale = d3scaleLog().base(base);
        } else
            this.scale = d3scaleLinear();
        this.scale
            .domain([min, max])
            .range([0, Resolution.legendBarWidth]);
        const ticks = Math.min(max, 10);
        this.xAxis = d3axisBottom(this.scale).ticks(ticks);
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
        this.hilightRect = canvas.append("rect")
            .attr("class", "dashed")
            .attr("height", this.barHeight)
            .attr("x", 0)
            .attr("y", 0)
            .attr("stroke-dasharray", "5,5")
            .attr("stroke", "cyan")
            .attr("fill", "none");
    }
}

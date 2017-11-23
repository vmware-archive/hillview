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

import {d3} from "./d3-modules"
import {IHtmlElement, Resolution, Size} from "./ui";
import {ContextMenu} from "./menu";

/**
 * Represents a map from the range 0-1 to colors.
 */
export class ColorMap {
    /**
     *  Suggested threshold for when a log-scale should be used.
     */
    public static logThreshold = 50;
    public logScale: boolean;

    public map: (x) => string = d3.interpolateWarm;

    constructor(public min = 0, public max = 1) {}

    public setLogScale(logScale: boolean) {
        this.logScale = logScale;
    }

    public apply(x: number) {
        if (this.logScale)
            return this.applyLog(x);
        else
            return this.applyLinear(x);
    }

    private applyLinear(x: number) {
        return this.map(x / this.max);
    }

    private applyLog(x: number) {
        return this.map(Math.log(x) / Math.log(this.max));
    }
}

/**
 * Displays a color map suitable for heatmaps.
 */
export class ColorLegend implements IHtmlElement {
    /* Static counter that increments to assign every ColorLegend object
       a unique ID for the gradient element. */
    private static nextUniqueId: number = 0;
    private uniqueId: number;
    private topLevel: HTMLElement;
    private gradient: any; // Element that contains the definitions for the colors in the color map

    private onColorMapChange: (ColorMap) => void; // Function that is called to update other elements when the color map changes.
    private contextMenu: ContextMenu;

    /**
     * Make a color legend for the given ColorMap with the specified parameters.
     * @param colorMap: ColorMap to make this legend for.
     * @param size: Size of the legend
     * @param barHeight: Height of the color bar rectangle.
     **/
    constructor(private colorMap: ColorMap,
                private size: Size = Resolution.legendSize,
                private barHeight = 16
    ) {
        this.uniqueId = ColorLegend.nextUniqueId++;
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("colorLegend");
    }

    public setColorMapChangeEventListener(listener: (ColorMap) => void) {
        this.onColorMapChange = listener;
        if (this.contextMenu == null)
            this.enableContextMenu();
    }

    /**
     * The context menu is added only when a colormap change event listener is set.
     */
    private enableContextMenu() {
        this.contextMenu = new ContextMenu(this.topLevel, [
            {
                text: "Cool",
                help: "Use a color palette with cool colors.",
                action: () => {
                    this.colorMap.map = d3.interpolateCool;
                    this.mapUpdated();
                }
            }, {
                text: "Warm",
                help: "Use a color palette with warm colors.",
                action: () => {
                    this.colorMap.map = d3.interpolateWarm;
                    this.mapUpdated();
                }
            }, {
                text: "Gray",
                help: "Use a grayscale color palette.",
                action: () => {
                    this.colorMap.map = (x: number) => `rgb(${Math.round(255 * (1 - x))}, ${Math.round(255 * (1 - x))}, ${Math.round(255 * (1 - x))})`;
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
        if (this.contextMenu != null){
            event.preventDefault();
            this.contextMenu.show(event);
        }
    }

    // Redraw the legend, and notify the listeners.
    private mapUpdated() {
        this.redraw();
        // Notify the onColorChange listener (redraw the elements with new colors)
        if (this.onColorMapChange != null)
            this.onColorMapChange(this.colorMap);
    }

    private base(): number {
        return this.colorMap.max > 10000 ? 10 : 2
    }

    private ticks(): number {
        return Math.min(this.colorMap.max, 10);
    }

    public getScale() {
        let scale;
        if (this.colorMap.logScale) {
            scale = d3.scaleLog()
                .base(this.base());
        } else
            scale = d3.scaleLinear();
        scale
            .domain([this.colorMap.min, this.colorMap.max])
            .range([0, this.size.width]);
        return scale;
    }

    public getAxis() {
        let scale = this.getScale();
        return d3.axisBottom(scale).ticks(this.ticks());
    }

    public redraw() {
        d3.select(this.topLevel).selectAll("svg").remove();
        let svg = d3.select(this.topLevel).append("svg")
            .attr("width", this.size.width)
            .attr("height", this.size.height);

        this.gradient = svg.append('defs')
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
                .attr("stop-color", this.colorMap.map(i / 100))
                .attr("stop-opacity", 1)

        let bar = svg.append("rect")
            .attr("x", 0)
            .attr("y", 0)
            .attr("width", this.size.width)
            .attr("height", this.barHeight)
            .style("fill", `url(#gradient${this.uniqueId})`);
        bar.on("contextmenu", () => this.showContextMenu(d3.event));

        let axisG = svg.append("g")
            .attr("transform", `translate(0, ${this.barHeight})`);

        let axis = this.getAxis();
        axisG.call(axis);
    }

    public getHTMLRepresentation() {
        return this.topLevel;
    }
}

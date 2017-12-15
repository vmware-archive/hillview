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

import {IHtmlElement, Size} from "./ui";
import {d3} from "./d3-modules";
import {TextOverlay} from "./textOverlay";
import {FullPage} from "./fullPage";

/**
 * A plotting surface contains an SVG element on top of which various charts are drawn.
 * There is a margin around the chart, which is dynamically computed.
 */
export class PlottingSurface implements IHtmlElement {
    topLevel: HTMLDivElement;
    /**
     * Number of pixels on between the top of the SVG area and the top of the drawn chart.
     */
    topMargin: number;
    /**
     * Number of pixels between the left of the SVG area and the left axis.
     */
    leftMargin: number;
    /**
     * Number of pixels between the bottom of the SVG area and the bottom axis.
     */
    bottomMargin: number;
    /**
     * Number of pixels between the right of the SVG area and the end of the drawn chart.
     */
    rightMargin: number;
    /**
     * SVG element on top of which the chart is drawn.
     */
    svgCanvas: any;
    /**
     * Current size in pixels of the svg element.
     */
    size: Size;
    /**
     * An AVG g element which is used to draw the chart; it is offset from the
     * svgCanvas by leftMargin, topMargin.
     */
    chartArea: any;
    /**
     * Describes the mouse pointer.  May be null.
     */
    pointDescription: TextOverlay;

    static readonly minCanvasWidth = 300; // minimum number of pixels for a plot (including margins)
    static readonly canvasHeight = 500;   // size of a plot
    static readonly topMargin = 10;        // top margin in pixels in a plot
    static readonly rightMargin = 20;     // right margin in pixels in a plot
    static readonly bottomMargin = 50;    // bottom margin in pixels in a plot
    static readonly leftMargin = 40;      // left margin in pixels in a plot

    constructor(parent: HTMLElement, public readonly page: FullPage, size?: Size) {
        this.topLevel = document.createElement("div");
        this.size = size;
        parent.appendChild(this.topLevel);
        this.clear();
    }

    // TODO: make private
    static getCanvasSize(page: FullPage): Size {
        let width = page.getWidthInPixels() - 3;
        if (width < PlottingSurface.minCanvasWidth)
            width = PlottingSurface.minCanvasWidth;
        return { width: width, height: PlottingSurface.canvasHeight };
    }

    // TODO: make private
    public static getChartSize(page: FullPage): Size {
        let canvasSize = PlottingSurface.getCanvasSize(page);
        let width = canvasSize.width - PlottingSurface.leftMargin - PlottingSurface.rightMargin;
        let height = canvasSize.height - PlottingSurface.topMargin - PlottingSurface.bottomMargin;
        return { width: width, height: height };
    }

    clear() {
        if (this.svgCanvas != null)
            this.svgCanvas.remove();
        if (this.size == null)
            this.size = PlottingSurface.getCanvasSize(this.page);
        this.svgCanvas = d3.select(this.topLevel)
            .append("svg")
            .attr("id", "canvas")
            .attr("border", 1)
            .attr("cursor", "crosshair")
            .attr("width", this.size.width)
            .attr("height", this.size.height);
        this.chartArea = this.svgCanvas.append("g");
        // TODO: compute these instead of having them be fixed
        this.setMargins(PlottingSurface.topMargin, PlottingSurface.rightMargin,
            PlottingSurface.bottomMargin, PlottingSurface.leftMargin);
    }

    getChart(): any {
        return this.chartArea;
    }

    getCanvas(): any {
        return this.svgCanvas;
    }

    /**
     * The width of the drawn chart, excluding the margins, in pixels.
     */
    getActualChartWidth(): number {
        return this.size.width - this.leftMargin - this.rightMargin;
    }

    /**
     * The height of the drawn chart, excluding the margins, in pixels.
     */
    getActualChartHeight(): number {
        return this.size.height - this.topMargin - this.bottomMargin;
    }

    getChartSize(): Size {
        return { width: this.getActualChartWidth(), height: this.getActualChartHeight() };
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    setMargins(top: number, right: number, bottom: number, left: number): void {
        this.topMargin = top;
        this.rightMargin = right;
        this.leftMargin = left;
        this.bottomMargin = bottom;
        this.chartArea
            .attr("transform", `translate(${this.leftMargin}, ${this.topMargin})`);
    }

    public reportError(message: string): void {
        this.page.reportError(message);
    }
}
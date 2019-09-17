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

import {select as d3select} from "d3-selection";
import {FullPage} from "./fullPage";
import {D3SvgElement, IHtmlElement, Size} from "./ui";

/**
 * An interface that can be used to specify various dimensions.
 * All dimensions are in pixels.
 */
export interface SizeAndBorders {
    width?: number;
    height?: number;
    topMargin?: number;
    leftMargin?: number;
    rightMargin?: number;
    bottomMargin?: number;
}

/**
 * A plotting surface contains an SVG element on top of which various charts are drawn.
 * There is a margin around the chart.
 */
export abstract class PlottingSurface {
    /**
     * Number of pixels on between the top of the SVG area and the top of the drawn chart.
     */
    public topMargin: number;
    /**
     * Number of pixels between the left of the SVG area and the left axis.
     */
    public leftMargin: number;
    /**
     * Number of pixels between the bottom of the SVG area and the bottom axis.
     */
    public bottomMargin: number;
    /**
     * Number of pixels between the right of the SVG area and the end of the drawn chart.
     */
    public rightMargin: number;
    /**
     * SVG element on top of which the chart is drawn.
     */
    public svgCanvas: D3SvgElement;
    /**
     * Current size in pixels of the canvas.
     */
    public size: Size;
    /**
     * An SVG g element which is used to draw the chart; it is offset from the
     * svgCanvas by leftMargin, topMargin.
     */
    public chartArea: D3SvgElement;
    protected selectionBorder: D3SvgElement;

    public static readonly minCanvasWidth = 300; // minimum number of pixels for a plot (including margins)
    public static readonly canvasHeight = 500;   // size of a plot
    public static readonly topMargin = 10;        // top margin in pixels in a plot
    public static readonly rightMargin = 20;     // right margin in pixels in a plot
    public static readonly bottomMargin = 50;    // bottom margin in pixels in a plot
    public static readonly leftMargin = 40;      // left margin in pixels in a plot

    protected constructor(public readonly page: FullPage, sb: SizeAndBorders) {
        // Default dimensions
        this.topMargin = PlottingSurface.topMargin;
        this.rightMargin = PlottingSurface.rightMargin;
        this.bottomMargin = PlottingSurface.bottomMargin;
        this.leftMargin = PlottingSurface.leftMargin;
        this.size = PlottingSurface.getDefaultCanvasSize(this.page.getWidthInPixels());
        this.size.width = Math.max(PlottingSurface.minCanvasWidth, this.size.width);
        // User-specified dimensions
        if (sb.topMargin != null)
            this.topMargin = sb.topMargin;
        if (sb.rightMargin != null)
            this.rightMargin = sb.rightMargin;
        if (sb.leftMargin != null)
            this.leftMargin = sb.leftMargin;
        if (sb.bottomMargin != null)
            this.bottomMargin = sb.bottomMargin;
        if (sb.width != null)
            this.size.width = sb.width;
        if (sb.height != null)
            this.size.height = sb.height;
    }

    public reportError(message: string): void {
        this.page.reportError(message);
    }

    public static getDefaultCanvasSize(pageWidth: number): Size {
        let width = pageWidth - 3;
        if (width < PlottingSurface.minCanvasWidth)
            width = PlottingSurface.minCanvasWidth;
        return { width, height: PlottingSurface.canvasHeight };
    }

    public static getDefaultChartSize(pageWidth: number): Size {
        const canvasSize = PlottingSurface.getDefaultCanvasSize(pageWidth);
        const width = canvasSize.width - PlottingSurface.leftMargin - PlottingSurface.rightMargin;
        const height = canvasSize.height - PlottingSurface.topMargin - PlottingSurface.bottomMargin;
        return { width, height };
    }

    /**
     * De-allocates the canvas if it exists.
     */
    public destroy(): void {
        if (this.svgCanvas != null)
            this.svgCanvas.remove();
        this.svgCanvas = null;
    }

    public getChart(): D3SvgElement {
        console.assert(this.chartArea != null);
        return this.chartArea;
    }

    /**
     * Get the canvas of the plotting area.  Must be called after create.
     */
    public getCanvas(): D3SvgElement {
        console.assert(this.svgCanvas != null);
        return this.svgCanvas;
    }

    /**
     * The width of the drawn chart, excluding the margins, in pixels.
     */
    public getChartWidth(): number {
        return this.size.width - this.leftMargin - this.rightMargin;
    }

    /**
     * The height of the drawn chart, excluding the margins, in pixels.
     */
    public getChartHeight(): number {
        return this.size.height - this.topMargin - this.bottomMargin;
    }

    public getActualChartSize(): Size {
        return { width: this.getChartWidth(), height: this.getChartHeight() };
    }

    public createChildSurface(xOffset: number, yOffset: number, sb: SizeAndBorders): PlottingSurface {
        const result = new SvgPlottingSurface(this.getCanvas().node(), xOffset, yOffset, this.page, sb);
        return result;
    }

    protected createObjects(root: D3SvgElement): void {
        this.svgCanvas = root
            .append("svg")
            .attr("id", "canvas")
            .attr("border", 1)
            .attr("cursor", "crosshair")
            .attr("width", this.size.width)
            .attr("height", this.size.height);
        this.chartArea = this.svgCanvas
            .append("g")
            .attr("transform", `translate(${this.leftMargin}, ${this.topMargin})`);
        this.selectionBorder = this.svgCanvas
            .append("rect")
            .attr("width", this.size.width)
            .attr("height", this.size.height)
            .attr("x", 0)
            .attr("y", 0)
            .attr("fill-opacity", .5)
            .attr("fill", "none");
    }

    public select(add: boolean): void {
        if (this.selectionBorder == null)
            return;
        if (add)
            this.selectionBorder
                .attr("fill", "pink");
        else
            this.selectionBorder
                .attr("fill", "none");
    }
}

export class HtmlPlottingSurface extends PlottingSurface implements IHtmlElement {
    public readonly topLevel: HTMLDivElement;

    constructor(parent: HTMLElement, public readonly page: FullPage, sb: SizeAndBorders) {
        super(page, sb);
        this.topLevel = document.createElement("div");
        parent.appendChild(this.topLevel);
        this.createObjects(d3select(this.topLevel));
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}

/**
 * A PlottingSurface that is hosted within another SVG element, shifted with
 * some offset.
 */
export class SvgPlottingSurface extends PlottingSurface {
    protected topLevel: SVGSVGElement;
    protected shifted: D3SvgElement;

    constructor(parent: HTMLElement,
                protected xOffset: number, protected yOffset: number,
                public readonly page: FullPage,
                sb: SizeAndBorders) {
        super(page, sb);
        this.topLevel = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        parent.appendChild(this.topLevel);
        this.shifted = d3select(this.topLevel)
            .append("g")
            .attr("transform", `translate(${this.xOffset}, ${this.yOffset})`);
        this.createObjects(this.shifted);
    }
}

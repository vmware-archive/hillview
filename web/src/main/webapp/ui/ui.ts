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

/**
 * This file contains various core classes and interfaces used
 * for building the Hillview TypeScript UI.
 */

import {FullPage} from "./fullPage";

/**
 * The list of rendering kinds supported by Hillview.
 */
export type ViewKind = "Table" | "Histogram" | "2DHistogram" | "Heatmap" |
    "Trellis" | "HeavyHitters" | "LAMP" | "Schema";

/**
 * Interface implemented by TypeScript objects that have an HTML rendering.
 * This returns the root of the HTML rendering.
 */
export interface IHtmlElement {
    getHTMLRepresentation() : HTMLElement;
}

/**
 * Interface implemented by TypeScript objects that are not HTML elements
 * but have a DOM representation.
 */
export interface IElement {
    getDOMRepresentation(): Element;
}

/**
 * A list of keyboard decoding values for keys used by Hillview.
 */
export enum KeyCodes {
    enter = 13,
    ctrl = 17,
    escape = 27,
    pageUp = 33,
    pageDown = 34,
    end = 35,
    home = 36
}

export const missingHtml: string = "<div class='missingData'>missing</div>";

export function textToDiv(text: string): HTMLElement {
    let div = document.createElement("div");
    div.innerHTML = text;
    return div;
}

/**
 * A list of special unicode character codes.
 */
export class SpecialChars {
    /// Approximation sign.
    public static approx = "\u2248";
}

/**
 * Remove all children of an HTML DOM object..
 */
export function removeAllChildren(h: HTMLElement): void {
    while (h.hasChildNodes())
        h.removeChild(h.lastChild);
}

/**
 * Size of a rectangle.
 */
export interface Size {
    width: number;
    height: number;
}

/**
 * Two-dimensional point.
 */
export interface Point {
    x: number;
    y: number;
}

export class PointSet {
    points: Point[];
}


/**
 * A rectangular surface.
 */
export class Rectangle {
    constructor(public readonly origin: Point, public readonly size: Size) {}
    upperLeft(): Point { return this.origin; }
    upperRight(): Point { return {
        x: this.origin.x + this.size.width,
        y: this.origin.y }; }
    lowerLeft(): Point { return {
        x: this.origin.x,
        y: this.origin.y + this.size.height }; }
    lowerRight(): Point { return {
        x: this.origin.x + this.size.width,
        y: this.origin.y + this.size.height }; }
    width(): number { return this.size.width; }
    height(): number { return this.size.height; }
}


/**
 * This class contains various constants for representing the dimensions of
 * the displayed objects.
 */
export class Resolution {
    static readonly maxBucketCount = 40;  // maximum number of buckets in a histogram
    static readonly minBarWidth = 5;      // minimum number of pixels for a histogram bar
    static readonly minCanvasWidth = 300; // minimum number of pixels for a plot (including margins)
    static readonly canvasHeight = 500;   // size of a plot
    static readonly topMargin = 60;       // top margin in pixels in a plot
    static readonly rightMargin = 20;     // right margin in pixels in a plot
    static readonly bottomMargin = 50;    // bottom margin in pixels in a plot
    static readonly leftMargin = 40;      // left margin in pixels in a plot
    static readonly minDotSize = 4;       // dots are drawn as rectangles of this size in pixels
    static readonly tableRowsOnScreen = 20; // table rows displayed
    static readonly lineHeight = 20;      // Height of a line of text drawn in svg (including reasonable margin).
    static readonly mouseDotRadius = 3;        // Size of dots that show mouse position
    static readonly legendSize: Size = {width: 500, height: 50}; // Default legend size in px.

    // Everything is drawn on top of the canvas.
    // The canvas includes a chart plus margins.
    public static getCanvasSize(page: FullPage): Size {
        let width = page.getWidthInPixels() - 3;
        if (width < Resolution.minCanvasWidth)
            width = Resolution.minCanvasWidth;
        return { width: width, height: Resolution.canvasHeight };
    }

    public static getChartSize(page: FullPage): Size {
        let canvasSize = Resolution.getCanvasSize(page);
        let width = canvasSize.width - Resolution.leftMargin - Resolution.rightMargin;
        let height = canvasSize.height - Resolution.topMargin - Resolution.bottomMargin;
        return { width: width, height: height };
    }
}

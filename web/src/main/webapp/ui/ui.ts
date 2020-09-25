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

/**
 * The list of rendering kinds supported by Hillview.
 */
import {ScaleLinear as D3ScaleLinear, ScaleTime as D3ScaleTime} from "d3-scale";
import {reorder} from "../util";

/**
 * HTML strings are strings that represent an HTML fragment.
 * They are usually assigned to innerHTML of a DOM object.
 * These strings should  be "safe": i.e., the HTML they contain should
 * be produced internally, and they should not come
 * from external sources, because they can cause DOM injection attacks.
 */
export class HtmlString {
    private safeValue: string;

    constructor(private arg: string) {
        // Escape the argument string.
        const div = document.createElement("div");
        div.innerText = arg;
        this.safeValue = div.innerHTML;
    }

    public appendSafeString(str: string): void {
        this.safeValue += str;
    }

    public append(message: HtmlString): void {
        this.safeValue += message.safeValue;
    }

    public prependSafeString(str: string): void {
        this.safeValue = str + this.safeValue;
    }

    public setInnerHtml(elem: HTMLElement): void {
        elem.innerHTML = this.safeValue;
    }
}

export type ViewKind = "Table" | "Histogram" | "2DHistogram" |
    "Heatmap" | "QuartileVector" | "CorrelationHeatmaps" |
    "TrellisHistogram" | "Trellis2DHistogram" | "TrellisHeatmap" | "TrellisQuartiles" |
    "HeavyHitters" | "Schema" | "Load" | "SVD Spectrum" | "LogFile" | "Map";

// Using an interface for emulating named arguments
// otherwise it's hard to remember the order of all these booleans.
export interface HistogramOptions {
    exact?: boolean;  // exact histogram
    reusePage: boolean;   // reuse the original page
    pieChart?: boolean;
}

export interface ChartOptions extends HistogramOptions {
    chartKind: ViewKind;
    relative?: boolean;  // draw a relative 2D histogram
    stacked?: boolean;   // stacked bars in 2D histograms
}

/**
 * Interface implemented by TypeScript objects that have an HTML rendering.
 * This returns the root of the HTML rendering.
 */
export interface IHtmlElement {
    getHTMLRepresentation(): HTMLElement;
}

/**
 * Interface implemented by TypeScript objects that are not HTML elements
 * but have a DOM representation.
 */
export interface IElement {
    getDOMRepresentation(): Element;
}

/**
 * A list of special unicode character codes.
 */
export class SpecialChars {
    // Approximation sign.
    public static approx = "\u2248";
    public static upArrow = "▲";
    public static downArrow = "▼";
    public static ellipsis = "…";
    public static downArrowHtml = "&dArr;";
    public static upArrowHtml = "&uArr;";
    public static leftArrowHtml = "&lArr;";
    public static rightArrowHtml = "&rArr;";
    public static epsilon = "\u03B5";
    public static enDash = "&ndash;";
}

/**
 * Remove all children of an HTML DOM object..
 */
export function removeAllChildren(h: HTMLElement): void {
    while (h.lastChild != null)
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
    public points: Point[];
}

/**
 * A rectangular surface.
 */
export class Rectangle {
    constructor(public readonly origin: Point, public readonly size: Size) {}
    public upperLeft(): Point { return this.origin; }
    public upperRight(): Point { return {
        x: this.origin.x + this.size.width,
        y: this.origin.y }; }
    public lowerLeft(): Point { return {
        x: this.origin.x,
        y: this.origin.y + this.size.height }; }
    public lowerRight(): Point { return {
        x: this.origin.x + this.size.width,
        y: this.origin.y + this.size.height }; }
    public width(): number { return this.size.width; }
    public height(): number { return this.size.height; }
    public inside(point: Point): boolean {
        if (point.x < this.origin.x || point.x > this.origin.x + this.size.width) return false;
        // noinspection RedundantIfStatementJS
        if (point.y < this.origin.y || point.y > this.origin.y + this.size.height) return false;
        return true;
    }
    public static fromCorners(corners: [[number, number], [number, number]]): Rectangle {
        const [x0, x1] = reorder(corners[0][0], corners[1][0]);
        const [y0, y1] = reorder(corners[0][1], corners[1][1]);
        return new Rectangle({x: x0, y: y0}, { width: x1 - x0, height: y1 - y0 });
    }
}

/**
 * This class contains various constants for representing the dimensions of
 * the displayed objects.
 */
export class Resolution {
    public static readonly max2DBucketCount = 25;  // maximum number of buckets stacked in a 2D histogram
    public static readonly minBarWidth = 15;     // minimum number of pixels for a histogram bar
    public static readonly minDotSize = 4;       // dots are drawn as rectangles of this size in pixels
    public static readonly tableRowsOnScreen = 20; // table rows displayed
    public static readonly lineHeight = 20;      // Height of a line of text drawn in svg (including reasonable margin).
    public static readonly mouseDotRadius = 3;        // Size of dots that show mouse position
    public static readonly legendBarWidth = 500;
    public static readonly legendSpaceHeight = 60;
    public static readonly minTrellisWindowSize = 200;
    public static readonly heatmapLabelWidth = 80;  // pixels reserved for heatmap label

    public static maxBuckets(pageWidth: number): number {
        return Math.floor(pageWidth / Resolution.minBarWidth);
    }
}

export type D3Axis = any;  // d3 axis; perhaps some day we will be able to use a better type
export type D3Scale = any; // d3 scale.
export type D3SvgElement = any;  // An SVG G element created by d3
export type AnyScale = D3ScaleLinear<number, number> | D3ScaleTime<number, number>;

// Kind of data that is being dragged
export type DragEventKind = "Title" | "XAxis" | "YAxis" | "GAxis";

/**
 * The format string used in a page title to describe a reference to another page.
 * @param page  Page number.
 */
export function pageReferenceFormat(page: string): string {
    return "%p(" + page + ")";
}

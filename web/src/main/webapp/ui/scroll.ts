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

import {drag as d3drag} from "d3-drag";
// import "d3-transition";
import {mouse as d3mouse, select as d3select} from "d3-selection";
import {percentString} from "../util";
import {D3SvgElement, IHtmlElement} from "./ui";

/**
 * A scroll target is an object which supports scrolling.  These are events
 * that are sent to the object when the user performs some actions.
 */
export interface IScrollTarget {
    scrolledTo(position: number): void;
    pageDown(): void;
    pageUp(): void;
}

/**
 * A custom scroll-bar.
 * The terminology is as if the scroll-bar is vertical,
 * but it can also be horizontal.
 */
export class ScrollBar implements IHtmlElement {
    private static readonly minimumSize = 10;
    private static readonly barWidth = 16;
    private static readonly handleHeight = 6;
    private static readonly handleWidth = 12;

    /**
     * Starting position; the range is 0-1.
     */
    public start: number;
    /**
     * End position: the range is 0-1.
     */
    public end: number;

    private readonly topLevel: HTMLElement;
    private beforeG: D3SvgElement;
    private before: D3SvgElement;
    private afterG: D3SvgElement;
    private after: D3SvgElement;
    private barG: D3SvgElement;
    private bar: D3SvgElement;
    private handleG: D3SvgElement;
    private handle: D3SvgElement;
    private svg: D3SvgElement;
    private height: number;
    private readonly target: IScrollTarget;

    private readonly heightAttr: string;
    private widthAttr: string;
    private xAttr: string;
    private readonly yAttr: string;

    /**
     * Build a scroll-bar.
     * @param target  Object that is notified when scrolling happens.
     * @param horizontal  If true the scroll-bar will be drawn horizontally.
     */
    constructor(target: IScrollTarget, private horizontal: boolean = false) {
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("scrollBar");
        this.target = target;
        this.topLevel.classList.add("hidden");
        this.height = 0;

        const drag = d3drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        if (this.horizontal) {
            this.heightAttr = "width";
            this.widthAttr = "height";
            this.xAttr = "y";
            this.yAttr = "x";
        } else {
            this.heightAttr = "height";
            this.widthAttr = "width";
            this.xAttr = "x";
            this.yAttr = "y";
        }

        this.svg = d3select(this.topLevel)
            .append("svg")
            .attr(this.widthAttr, ScrollBar.barWidth)
            .attr(this.heightAttr, "100%");

        this.beforeG = this.svg.append("g");
        this.beforeG
            .append("svg:title")
            .text("Clicking in this area will move one page " + (this.horizontal ? "left" : "up"));
        this.before = this.beforeG
            .append("rect")
            .attr(this.widthAttr, "100%")
            .attr(this.heightAttr, 0)
            .attr(this.xAttr, 0)
            .attr(this.yAttr, 0)
            .attr("fill", "lightgrey")
            .on("click", () => this.target.pageUp());
        this.afterG = this.svg.append("g");
        this.afterG
            .append("svg:title")
            .text("Clicking in this area will move one page " + (this.horizontal ? "right" : "down"));
        this.after = this.afterG
            .append("rect")
            .attr(this.widthAttr, "100%")
            .attr(this.heightAttr, 0)
            .attr(this.xAttr, 0)
            .attr(this.yAttr, 0)
            .attr("fill", "lightgrey")
            .on("click", () => this.target.pageDown());
        // This is drawn last; it may overlap with the other two
        // if we force its dimension to be minimumSize
        this.barG = this.svg.append("g");
        this.barG
            .append("svg:title");
        this.bar = this.barG
            .append("rect")
            .attr(this.widthAttr, "100%")
            .attr(this.heightAttr, 0)
            .attr(this.xAttr, 0)
            .attr(this.yAttr, 0)
            .attr("fill", "darkgrey");

        this.handleG = this.svg.append("g");
        this.handleG
            .append("svg:title")
            .text("Drag this handle with the mouse to scroll.");
        this.handle = this.handleG
            .append("rect")
            .attr(this.widthAttr, "80%")
            .attr(this.heightAttr, 6)
            .attr(this.xAttr, (ScrollBar.barWidth - ScrollBar.handleWidth) / 2)
            .attr(this.yAttr, 0)
            .attr("stroke", "black")
            .attr("stroke-width", 1)
            .attr("fill", "darkgrey")
            .attr("cursor", this.horizontal ? "ew-resize" : "ns-resize")
            .call(drag);
    }

    public dragStart(): void {
        this.bar.attr(this.heightAttr, "0");
        this.before.attr(this.heightAttr, this.height);
        this.before.attr(this.yAttr, 0);
    }

    public dragEnd(): void {
        const position = this.handle.attr(this.yAttr);
        let perc = position / this.height;
        if (position >= this.height - ScrollBar.handleHeight)
            perc = 1;
        if (this.target != null)
            this.target.scrolledTo(perc);
    }

    public dragMove(): void {
        const position = d3mouse(this.svg.node());
        let y;
        if (this.horizontal)
            y = position[0];
        else
            y = position[1];
        if (y < 0)
            y = 0;
        if (y > this.height - ScrollBar.handleHeight)
            y = this.height - ScrollBar.handleHeight;
        this.handle.attr(this.yAttr, y);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public computePosition(): void {
        if (this.start <= 0.0 && this.end >= 1.0) {
            this.topLevel.classList.add("hidden");
            return;
        } else {
            this.topLevel.classList.remove("hidden");
        }

        this.height = this.horizontal ?
            this.topLevel.getBoundingClientRect().width :
            this.topLevel.getBoundingClientRect().height;
        let barHeight = (this.end - this.start) * this.height;
        let barY = this.start * this.height;
        if (barHeight < ScrollBar.minimumSize) {
            barHeight = ScrollBar.minimumSize;
            if (barY + barHeight > this.height)
                barY = this.height - barHeight;
        }
        this.before
            .attr(this.heightAttr, this.start * this.height);
        this.barG
            .select("title")
            .text(percentString(this.start) + " - " + percentString(this.end));
        this.bar
            .attr(this.heightAttr, barHeight)
            .attr(this.yAttr, barY);
        this.after
            .attr(this.heightAttr, (1 - this.end) * this.height)
            .attr(this.yAttr, this.end * this.height);
        // handle in the middle of the bar
        this.handle
            .attr(this.yAttr, barY + ((barHeight - ScrollBar.handleHeight) / 2));
    }

    public setPosition(start: number, end: number): void {
        if (start > end)
            throw new Error("Start after end: " + start + "/" + end);
        this.start = start;
        this.end = end;
        this.computePosition();
    }
}

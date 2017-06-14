/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {RpcReceiver, PartialResult, ICancellable} from "./rpc";
import {ErrorReporter} from "./errReporter";
import d3 = require('d3');

export interface IHtmlElement {
    getHTMLRepresentation() : HTMLElement;
}

export enum KeyCodes {
    escape = 27,
    pageUp = 33,
    pageDown = 34,
    end = 35,
    home = 36
}

export interface Size {
    width: number;
    height: number;
}

export interface Point {
    x: number;
    y: number;
}

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
}

export interface HillviewDataView extends IHtmlElement {
    setPage(page: FullPage): void;
    getPage(): FullPage;
    refresh(): void;
}

export function getWindowSize(): Size {
    return {
        width: window.innerWidth,
        height: window.innerHeight
    };
}

export function formatNumber(n: number): string {
    return n.toLocaleString();
}

export function percent(n: number): string {
    n = Math.round(n * 1000) / 10;
    return significantDigits(n) + "%";
}

// Generates a string that encodes a call to the SVG translate method
export function translateString(x: number, y: number): string {
    return "translate(" + String(x) + ", " + String(y) + ")";
}

export function significantDigits(n: number): string {
    let suffix = "";
    let absn = Math.abs(n);
    if (absn > 1e12) {
        suffix = "T";
        n = n / 1e12;
    } else if (absn > 1e9) {
        suffix = "B";
        n = n / 1e9;
    } else if (absn > 1e6) {
        suffix = "M";
        n = n / 1e6;
    } else if (absn > 5e3) {
        // This will prevent many year values from being converted
        suffix = "K";
        n = n / 1e3;
    } else if (absn < 1e-12) {
        n = 0;
    } else if (absn < 1e-9) {
        suffix = "n";
        n = n * 1e9;
    } else if (absn < 1e-6) {
        suffix = "u";
        n = n * 1e6;
    } else if (absn < 1e-3) {
        suffix = "m";
        n = n * 1e3;
    }
    n = Math.round(n * 100) / 100;
    return String(n) + suffix;
}

export interface IScrollTarget {
    scrolledTo(position: number): void;
    pageDown(): void;
    pageUp(): void;
}

export function removeAllChildren(h: HTMLElement): void {
    while (h.hasChildNodes())
        h.removeChild(h.lastChild);
}

export class ScrollBar implements IHtmlElement {
    static readonly minimumSize = 10;
    static readonly barWidth = 16;
    static readonly handleHeight = 6;
    static readonly handleWidth = 12;

    // Only vertical scroll bars supported
    // Range for start and end is 0-1
    start : number;
    end : number;

    private topLevel: HTMLElement;
    private before: any;
    private after: any;
    private bar: any;
    private handle: any;
    private svg: any;
    private height: number;
    private target: IScrollTarget;

    constructor(target: IScrollTarget) {
        this.topLevel = document.createElement("div");
        this.target = target;
        this.topLevel.style.visibility = 'hidden';
        this.height = 0;

        let drag = d3.drag()
            .on("start", () => this.dragStart())
            .on("drag", () => this.dragMove())
            .on("end", () => this.dragEnd());

        this.svg = d3.select(this.topLevel)
            .append("svg")
            .attr("width", ScrollBar.barWidth)
            .attr("height", "100%");

        this.before = this.svg.append("rect")
            .attr("width", "100%")
            .attr("height", 0)
            .attr("x", 0)
            .attr("y", 0)
            .attr("fill", "lightgrey")
            .on("click", () => this.target.pageUp());
        this.after = this.svg.append("rect")
            .attr("width", "100%")
            .attr("height", 0)
            .attr("x", 0)
            .attr("y", 0)
            .attr("fill", "lightgrey")
            .on("click", () => this.target.pageDown());
        // This is drawn last; it may overlap with the other two
        // if we force its dimension to be minimumSize
        this.bar = this.svg.append("rect")
            .attr("width", "100%")
            .attr("height", 0)
            .attr("x", 0)
            .attr("y", 0)
            .attr("fill", "darkgrey");

        this.handle = this.svg
            .append("rect")
            .attr("width", "80%")
            .attr("height", 6)
            .attr("x", (ScrollBar.barWidth - ScrollBar.handleWidth) / 2)
            .attr("y", 0)
            .attr("stroke", "black")
            .attr("stroke-width", 1)
            .attr("fill", "darkgrey")
            .attr("cursor", "ns-resize")
            .call(drag);
    }

    dragStart(): void {
        this.bar.attr("height", "0");
        this.before.attr("height", this.height);
        this.before.attr("y", 0);
    }

    dragEnd(): void {
        let position = this.handle.attr("y");
        let perc = position / this.height;
        if (position >= this.height - ScrollBar.handleHeight)
            perc = 1;
        if (this.target != null)
            this.target.scrolledTo(perc);
    }

    dragMove(): void {
        let position = d3.mouse(this.svg.node());
        let y = position[1];
        if (y < 0)
            y = 0;
        if (y > this.height - ScrollBar.handleHeight)
            y = this.height - ScrollBar.handleHeight;
        this.handle.attr("y", y);
    }

    getHTMLRepresentation() : HTMLElement {
        return this.topLevel;
    }

    computePosition() : void {
        if (this.start <= 0.0 && this.end >= 1.0) {
            this.topLevel.style.visibility = 'hidden';
            return;
        } else {
            this.topLevel.style.visibility = 'visible';
        }

        this.height = this.topLevel.getBoundingClientRect().height;
        let barHeight = (this.end - this.start) * this.height;
        let barY = this.start * this.height;
        if (barHeight < ScrollBar.minimumSize) {
            barHeight = ScrollBar.minimumSize;
            if (barY + barHeight > this.height)
                barY = this.height - barHeight;
        }
        this.before
            .attr("height", this.start * this.height);
        this.bar
            .attr("height", barHeight)
            .attr("y", barY);
        this.after
            .attr("height", (1 - this.end) * this.height)
            .attr("y", this.end * this.height);
        // handle in the middle of the bar
        this.handle
            .attr("y", barY + ((barHeight - ScrollBar.handleHeight) / 2));
    }

    setPosition(start : number, end: number) : void {
        if (start > end)
            throw "Start after end: " + start + "/" + end;
        this.start = start;
        this.end = end;
        this.computePosition();
    }
}

export class ProgressBar implements IHtmlElement {
    end: number;

    private finished : boolean;
    private bar      : HTMLElement;
    private topLevel : HTMLElement;

    constructor(private manager: ProgressManager,
                public readonly description: string,
                private readonly operation: ICancellable) {  // may be null
        if (description == null)
            throw "Null label";
        if (manager == null)
            throw "Null ProgressManager";

        this.finished = false;
        let top = document.createElement("table");
        top.className = "noBorder";
        this.topLevel = top;
        let body = top.createTBody();
        let row = body.insertRow();
        row.className = "noBorder";

        let cancelButton = document.createElement("button");
        cancelButton.textContent = "Stop";
        let label = document.createElement("div");
        label.textContent = description;
        label.className = "label";

        let outer = document.createElement("div");
        outer.className = "progressBarOuter";

        this.bar = document.createElement("div");
        this.bar.className = "progressBarInner";

        outer.appendChild(this.bar);

        let labelCell = row.insertCell(0);
        labelCell.appendChild(label);
        labelCell.style.textAlign = "left";
        labelCell.className = "noBorder";

        let barCell = row.insertCell(1);
        barCell.appendChild(outer);
        barCell.className = "noBorder";

        let buttonCell = row.insertCell(2);
        buttonCell.appendChild(cancelButton);
        buttonCell.className = "noBorder";

        this.setPosition(0.0);
        cancelButton.onclick = () => this.cancel();
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    setPosition(end: number) : void {
        if (this.finished)
            // One may attempt to update the progress bar
            // even after completion
            return;
        if (end < 0)
            end = 0;
        if (end > 1)
            end = 1;
        if (end < this.end)
            console.log("Progress bar moves backward:" + this.end + " to " + end);
        this.end = end;
        this.computePosition();
    }

    computePosition() : void {
        this.bar.style.width = String(this.end * 100) + "%";
    }

    setFinished() : void {
        if (this.finished)
            return;
        this.setPosition(1.0);
        this.finished = true;
        this.manager.removeProgressBar(this);
    }

    cancel(): void {
        if (this.operation != null)
            this.operation.cancel();
        this.setFinished();
    }
}

// This class manages multiple progress bars.
export class ProgressManager implements IHtmlElement {
    topLevel: HTMLElement;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "progressManager";
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    newProgressBar(operation: ICancellable, description: string) {
        let p = new ProgressBar(this, description, operation);
        this.topLevel.appendChild(p.getHTMLRepresentation());
        return p;
    }

    removeProgressBar(p: ProgressBar) {
        this.topLevel.removeChild(p.getHTMLRepresentation());
    }
}

// Here we display the main visualization
export class DataDisplay implements IHtmlElement {
    topLevel: HTMLElement;
    element: HillviewDataView;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "dataDisplay";
    }

    public onResize(): void {
        if (this.element != null)
            this.element.refresh();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public setHillviewDataView(element: HillviewDataView): void {
        this.element = element;
        removeAllChildren(this.topLevel);
        this.topLevel.appendChild(element.getHTMLRepresentation());
    }
}

export class ConsoleDisplay implements IHtmlElement, ErrorReporter {
    topLevel: HTMLElement;

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "console";
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public reportError(message: string): void {
        this.topLevel.textContent += message;
    }

    public clear(): void {
        this.topLevel.textContent = "";
    }
}

// A page is divided into several sections:
// - the data display
// - the progress manager
// - the console
export class FullPage implements IHtmlElement {
    public dataDisplay: DataDisplay;
    bottomContainer: HTMLElement;
    public progressManager: ProgressManager;
    protected console: ConsoleDisplay;
    pageTopLevel: HTMLElement;
    static pageCounter: number = 0;
    protected pageId: number;

    // All visible pages are children of a div named 'top'.
    static allPages: FullPage[] = [];  // should be the same as the children of top 'div'

    public constructor() {
        this.pageId = FullPage.pageCounter++;
        this.console = new ConsoleDisplay();
        this.progressManager = new ProgressManager();
        this.dataDisplay = new DataDisplay();

        this.pageTopLevel = document.createElement("div");
        this.pageTopLevel.className = "hillviewPage";
        this.bottomContainer = document.createElement("div");
        let close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.onclick = (e) => this.remove();
        this.pageTopLevel.appendChild(close);
        this.pageTopLevel.appendChild(this.dataDisplay.getHTMLRepresentation());
        this.pageTopLevel.appendChild(this.bottomContainer);

        this.bottomContainer.appendChild(this.progressManager.getHTMLRepresentation());
        this.bottomContainer.appendChild(this.console.getHTMLRepresentation());
    }

    protected static getTop(): HTMLElement {
        return document.getElementById('top');
    }

    public append(): void {
        let top = FullPage.getTop();
        FullPage.allPages.push(this);
        top.appendChild(this.getHTMLRepresentation());
    }

    public findIndex(): number {
        let index = FullPage.allPages.indexOf(this);
        if (index < 0)
            throw "Page to insert after not found";
        return index;
    }

    public insertAfterMe(page: FullPage): void {
        let index = this.findIndex();
        FullPage.allPages.splice(index+1, 0, page);
        let top = FullPage.getTop();
        let pageRepresentation = page.getHTMLRepresentation();
        if (index >= top.children.length - 1)
            top.appendChild(pageRepresentation);
        else
            top.insertBefore(pageRepresentation, top.children[index+1]);
        pageRepresentation.scrollIntoView( { block: "end", behavior: "smooth" } );
    }

    public remove(): void {
        let index = this.findIndex();
        FullPage.allPages.splice(index, 1);
        let top = FullPage.getTop();
        top.removeChild(top.children[index]);
    }

    public onResize(): void {
        this.dataDisplay.onResize();
    }

    public static onResize(): void {
        if (FullPage.allPages != null)
            for (let p of FullPage.allPages)
                p.onResize();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.pageTopLevel;
    }

    public getErrorReporter(): ErrorReporter {
        return this.console;
    }

    public setHillviewDataView(hdv: HillviewDataView): void {
        this.dataDisplay.setHillviewDataView(hdv);
    }

    public reportError(error: string) {
        this.getErrorReporter().clear();
        this.getErrorReporter().reportError(error);
    }

    public getWidthInPixels(): number {
        return getWindowSize().width;
    }
}

export abstract class Renderer<T> extends RpcReceiver<PartialResult<T>> {
    public constructor(public page: FullPage,
                       public operation: ICancellable,
                       public description: string) {
        super(page.progressManager.newProgressBar(operation, description),
              page.getErrorReporter());
        // TODO: This may be too eager.
        page.getErrorReporter().clear();
    }

    public onNext(value: PartialResult<T>) {
        this.progressBar.setPosition(value.done);
    }

    public elapsedMilliseconds(): number {
        return d3.timeMillisecond.count(this.operation.startTime(), new Date());
    }
}
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

export interface IHtmlElement {
    getHTMLRepresentation() : HTMLElement;
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

export interface HieroDataView extends IHtmlElement {
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
    } else if (absn > 1e3) {
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

export function removeAllChildren(h: HTMLElement): void {
    while (h.hasChildNodes())
        h.removeChild(h.lastChild);
}

export class ScrollBar implements IHtmlElement {
    // Only vertical scroll bars supported
    // Range for start and end is 0-1
    start : number;
    end : number;

    private outer  : HTMLElement;
    private inner  : HTMLElement;
    private before : HTMLElement;
    private after  : HTMLElement;

    constructor() {
        this.outer = document.createElement("div");
        this.outer.className = "scrollbarOuter";

        this.inner = document.createElement("div");
        this.inner.className = "scrollBarInner";

        this.before = document.createElement("div");
        this.before.className = "scrollBarBefore";

        this.after = document.createElement("div");
        this.after.className = "scrollBarAfter";

        this.outer.appendChild(this.before);
        this.outer.appendChild(this.inner);
        this.outer.appendChild(this.after);
        this.setPosition(0, 1);
    }

    getHTMLRepresentation() : HTMLElement {
        return this.outer;
    }

    computePosition() : void {
        if (this.start <= 0.0 && this.end >= 1.0)
            this.outer.style.visibility = 'hidden';
        else
            this.outer.style.visibility = 'visible';
        this.before.style.height = String(this.start * 100) + "%";
        this.inner.style.height = String((this.end - this.start) * 100) + "%";
        this.after.style.height = String((1 - this.end) * 100) + "%";
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
                private readonly operation: ICancellable) {
        if (operation == null)
            throw "Null operation";
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
        labelCell.className = "leftAlign noBorder";

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
    element: HieroDataView;

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

    public setHieroDataView(element: HieroDataView): void {
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
    topLevel: HTMLElement;

    public constructor() {
        this.console = new ConsoleDisplay();
        this.progressManager = new ProgressManager();
        this.dataDisplay = new DataDisplay();

        this.topLevel = document.createElement("div");
        this.bottomContainer = document.createElement("div");
        this.topLevel.appendChild(this.dataDisplay.getHTMLRepresentation());
        this.topLevel.appendChild(this.bottomContainer);

        this.bottomContainer.appendChild(this.progressManager.getHTMLRepresentation());
        this.bottomContainer.appendChild(this.console.getHTMLRepresentation());
    }

    public onResize(): void {
        this.dataDisplay.onResize();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public getErrorReporter(): ErrorReporter {
        return this.console;
    }

    public setHieroDataView(hdv: HieroDataView): void {
        this.dataDisplay.setHieroDataView(hdv);
    }

    public reportError(error: string) {
        this.getErrorReporter().clear();
        this.getErrorReporter().reportError(error);
    }
}

export interface MenuItem {
    text: string;
    action: () => void;
}

export class ContextMenu implements IHtmlElement {
    items: MenuItem[];
    private outer: HTMLElement;
    private htmlTable: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;

    constructor(mis: MenuItem[]) {
        this.outer = document.createElement("div");
        this.outer.className = "dropdown";
        this.outer.onmouseout = () => this.toggleVisibility();
        this.htmlTable = document.createElement("table");
        this.outer.appendChild(this.htmlTable);
        this.tableBody = this.htmlTable.createTBody();
        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    toggleVisibility(): void {
        this.outer.classList.toggle("shown");
    }

    addItem(mi: MenuItem): void {
        this.items.push(mi);
        let trow = this.tableBody.insertRow();
        let cell = trow.insertCell(0);
        cell.innerHTML = mi.text;
        cell.className = "menuItem";
        cell.onclick = () => { this.toggleVisibility(); mi.action(); }
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
    }
}

interface SubMenu {
    readonly text: string;
    readonly subMenu: ContextMenu;
}

export class DropDownMenu implements IHtmlElement {
    items: SubMenu[];
    private outer: HTMLElement;
    private htmlTable: HTMLTableElement;
    private tableBody: HTMLTableSectionElement;
    private tableRow: HTMLTableRowElement;

    constructor(mis: SubMenu[]) {
        this.outer = document.createElement("div");
        this.htmlTable = document.createElement("table");
        this.outer.appendChild(this.htmlTable);
        this.tableBody = this.htmlTable.createTBody();
        this.tableRow = this.tableBody.insertRow();
        this.items = [];
        if (mis != null) {
            for (let mi of mis)
                this.addItem(mi);
        }
    }

    addItem(mi: SubMenu): void {
        this.items.push(mi);
        let cell = this.tableRow.insertCell();
        cell.innerHTML = mi.text;
        cell.className = "menuItem";
        cell.onclick = () => { mi.subMenu.toggleVisibility(); }
    }

    getHTMLRepresentation(): HTMLElement {
        return this.outer;
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
}
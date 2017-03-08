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
import {ErrorReporter} from "./errorReporter";

export interface IHtmlElement {
    getHTMLRepresentation() : HTMLElement;
}

export interface HieroDataView extends IHtmlElement {
    setPage(page: FullPage): void;
    getPage(): FullPage;
}

function removeAllChildren(h: HTMLElement): void {
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
    private outer    : HTMLElement;
    private bar      : HTMLElement;
    private topLevel : HTMLElement;
    private cancelButton : HTMLButtonElement;
    private label    : HTMLElement;

    constructor(private manager: ProgressManager,
                public readonly lab: string,
                private readonly operation: ICancellable) {
        if (operation == null)
            throw "Null operation";
        if (lab == null)
            throw "Null label";
        if (manager == null)
            throw "Null ProgressManager";

        this.finished = false;
        this.topLevel = document.createElement("div");
        this.cancelButton = document.createElement("button");
        this.cancelButton.textContent = "Stop";
        this.label = document.createElement("div");
        this.label.textContent = lab;

        this.outer = document.createElement("div");
        this.outer.className = "progressBarOuter";

        this.bar = document.createElement("div");
        this.bar.className = "progressBarInner";

        this.outer.appendChild(this.bar);
        this.topLevel.appendChild(this.outer);
        this.topLevel.appendChild(this.cancelButton);
        this.topLevel.appendChild(this.label);
        this.topLevel.className = "flexcontainer";

        this.setPosition(0.0);
        this.cancelButton.onclick = () => this.cancel();
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

    newProgressBar(operation: ICancellable, message: string) {
        let p = new ProgressBar(this, message, operation);
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

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.className = "dataDisplay";
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public setHieroDataView(element: HieroDataView): void {
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

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    public getErrorReporter(): ErrorReporter {
        return this.console;
    }

    public setHieroDataView(hdv: HieroDataView): void {
        this.dataDisplay.setHieroDataView(hdv);
    }
}

export class MenuItem {
    text: string;
    action: () => void;
}

export class Menu implements IHtmlElement {
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

export abstract class Renderer<T> extends RpcReceiver<PartialResult<T>> {
    public constructor(public bar: ProgressBar, reporter?: ErrorReporter) {
        super(bar, reporter);
    }
}
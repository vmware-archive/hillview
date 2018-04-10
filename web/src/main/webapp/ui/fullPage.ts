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

import {IHtmlElement, removeAllChildren, ViewKind} from "./ui";
import {ProgressManager} from "./progress";
import {TopMenu} from "./menu";
import {openInNewTab, significantDigits} from "../util";
import {ConsoleDisplay, ErrorReporter} from "./errReporter";
import {DataDisplay, IDataView} from "./dataview";
import {Dataset} from "../dataset";

/**
 * Maps each ViewKind to a url anchor in the github userManual.
 */
let helpUrl = {
    "Table": "table-views",
    "Histogram": "uni-dimensional-histogram-views",
    "2DHistogram": "two-dimensional-histogram-views",
    "Heatmap": "heatmap-views",
    "Trellis": "trellis-plot-views",
    "HeavyHitters": "heavy-hitter-views",
    "LAMP": "lamp-projection",
    "Schema": "data-schema-views",
    "Load": "loading-data",
    "SVD Spectrum": "svd-spectrum"
};

const minus = "&#8722;";
const plus = "+";

/**
 * A FullPage is the main unit of display in Hillview, storing on rendering.
 * The page layout is as follows:
 * -------------------------------------------------
 * | menu           Title                 ? - [2] x|  titleRow
 * |-----------------------------------------------|
 * |                                               |
 * |                 data display                  |  displayHolder
 * |                                               |
 * -------------------------------------------------
 * | progress manager (reports progress)           |
 * | console          (reports errors)             |
 * -------------------------------------------------
 */
export class FullPage implements IHtmlElement {
    public dataDisplay: DataDisplay;
    bottomContainer: HTMLElement;
    public progressManager: ProgressManager;
    protected console: ConsoleDisplay;
    pageTopLevel: HTMLElement;
    private readonly menuSlot: HTMLElement;
    private readonly h1: HTMLElement;
    private minimized: boolean;
    private readonly displayHolder: HTMLElement;
    protected titleRow: HTMLDivElement;
    protected help: HTMLElement;

    /**
     * Creates a page which will be used to display some rendering.
     * @param pageId      Page number within dataset.
     * @param title       Title to use for page.
     * @param sourcePage  Page which initiated the creation of this one.
     * @param dataset     Parent dataset; only null for the toplevel menu.
     */
    public constructor(public readonly pageId: number, public readonly title: string,
                       sourcePage: FullPage, public readonly dataset: Dataset) {
        this.console = new ConsoleDisplay();
        this.progressManager = new ProgressManager();
        this.dataDisplay = new DataDisplay();
        this.minimized = false;

        this.pageTopLevel = document.createElement("div");
        this.pageTopLevel.className = "hillviewPage";
        this.pageTopLevel.id = "hillviewPage" + this.pageId.toString();
        this.bottomContainer = document.createElement("div");

        this.titleRow = document.createElement("div");
        this.titleRow.style.display = "flex";
        this.titleRow.style.width = "100%";
        this.titleRow.style.flexDirection = "row";
        this.titleRow.style.flexWrap = "nowrap";
        this.titleRow.style.alignItems = "center";
        this.pageTopLevel.appendChild(this.titleRow);
        this.menuSlot = document.createElement("div");
        this.addCell(this.menuSlot, true);

        let h1 = document.createElement("h1");
        if (title != null)
            h1.innerHTML = (this.pageId > 0 ? (this.pageId.toString() + ". ") : "") + title;
        h1.style.textOverflow = "ellipsis";
        h1.style.textAlign = "center";
        h1.style.margin = "0";
        this.h1 = h1;
        this.addCell(h1, false);

        if (sourcePage != null) {
            h1.innerHTML += " from ";
            let refLink = this.pageReference(sourcePage.pageId);
            refLink.title = "View which produced this one.";
            h1.appendChild(refLink);
        }

        this.help = document.createElement("button");
        this.help.textContent = "?";
        this.help.className = "help";
        this.help.title = "Open help documentation related to this view.";
        this.addCell(this.help, true);

        if (this.dataset != null) {
            // The load menu does not have these decorative elements
            let minimize = document.createElement("span");
            minimize.className = "minimize";
            minimize.innerHTML = minus;
            minimize.onclick = () => this.minimize(minimize);
            minimize.title = "Minimize this view.";
            this.addCell(minimize, true);

            /*
            let pageIdSpan = document.createElement("span");
            pageIdSpan.textContent = "[" + this.pageId + "]";
            pageIdSpan.title = "Unique number of this view.";
            pageIdSpan.draggable = true;
            pageIdSpan.ondragstart = (e) => e.dataTransfer.setData("text", this.pageId.toString());
            this.addCell(pageIdSpan, true);
            */

            let close = document.createElement("span");
            close.className = "close";
            close.innerHTML = "&times;";
            close.onclick = () => this.dataset.remove(this);
            close.title = "Close this view.";
            this.addCell(close, true);
        }

        this.displayHolder = document.createElement("div");
        this.pageTopLevel.appendChild(this.displayHolder);
        this.displayHolder.appendChild(this.dataDisplay.getHTMLRepresentation());
        this.pageTopLevel.appendChild(this.bottomContainer);

        this.bottomContainer.appendChild(this.progressManager.getHTMLRepresentation());
        this.bottomContainer.appendChild(this.console.getHTMLRepresentation());
    }

    public setViewKind(viewKind: ViewKind): void {
        this.help.onclick = () => openInNewTab(FullPage.helpUrl(viewKind));
    }

    getTitleElement(): HTMLElement {
        return this.h1;
    }

    /**
     * Returns a URL to a section in the user manual.
     * @param {ViewKind} viewKind  Kind of view that help is sought for.
     */
    static helpUrl(viewKind: ViewKind): string {
        let ref = helpUrl[viewKind];
        return "https://github.com/vmware/hillview/blob/master/docs/userManual.md#" + ref;
    }

    /**
     * @eturns An html string that represents a reference to the specified page.
     */
    pageReference(pageId: number): HTMLElement {
        let refLink = document.createElement("a");
        refLink.href = "#";
        refLink.textContent = pageId.toString();
        refLink.onclick = () => {
            if (!this.dataset.scrollIntoView(pageId))
                this.reportError("Page " + pageId + " no longer exists");
            // return false to prevent the url from being followed.
            return false;
        };
        return refLink;
    }

    setMenu(c: TopMenu): void {
        removeAllChildren(this.menuSlot);
        this.menuSlot.appendChild(c.getHTMLRepresentation());
    }

    addCell(c: HTMLElement, minSize: boolean): void {
        if (minSize != null && minSize)
            c.style.flexGrow = "1";
        else
            c.style.flexGrow = "100";
        this.titleRow.appendChild(c);
    }

    public minimize(span: HTMLElement): void {
        if (this.minimized) {
            this.displayHolder.appendChild(this.dataDisplay.getHTMLRepresentation());
            this.minimized = false;
            span.innerHTML = minus;
        } else {
            removeAllChildren(this.displayHolder);
            this.clearError();
            this.minimized = true;
            span.innerHTML = plus;
        }
    }

    public onResize(): void {
        this.dataDisplay.onResize();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.pageTopLevel;
    }

    public getErrorReporter(): ErrorReporter {
        return this.console;
    }

    public setDataView(hdv: IDataView): void {
        this.dataDisplay.setDataView(hdv);
        this.setViewKind(hdv.viewKind);
    }

    public reportError(error: string) {
        this.getErrorReporter().clear();
        this.getErrorReporter().reportError(error);
    }

    public clearError() {
        this.getErrorReporter().clear();
    }

    public reportTime(timeInMs: number) {
        this.reportError("Operation took " + significantDigits(timeInMs/1000) + " seconds");
    }

    public getWidthInPixels(): number {
        return this.pageTopLevel.getBoundingClientRect().width;
    }

    scrollIntoView(): void {
        this.getHTMLRepresentation().scrollIntoView( { block: "end", behavior: "smooth" } );
    }
}

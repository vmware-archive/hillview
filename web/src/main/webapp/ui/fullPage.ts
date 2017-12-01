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
import {significantDigits} from "../util";
import {ConsoleDisplay, ErrorReporter} from "./errReporter";
import {DataDisplay, IDataView} from "./dataview";

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
    "Load": "loading-data"
};

/**
 * A FullPage is the main unit of display in Hillview, storing on rendering.
 * The page layout is as follows:
 * -------------------------------------------------
 * | menu  help         Title                     x|
 * |-----------------------------------------------|
 * |                                               |
 * |                 data display                  |
 * |                                               |
 * -------------------------------------------------
 * | progress manager (reports progress)           |
 * | console          (reports errors              |
 * --------------------------------------------------
 */
export class FullPage implements IHtmlElement {
    public dataDisplay: DataDisplay;
    bottomContainer: HTMLElement;
    public progressManager: ProgressManager;
    protected console: ConsoleDisplay;
    pageTopLevel: HTMLElement;
    static pageCounter: number = 0;
    public readonly pageId: number;
    private menuSlot: HTMLElement;

    /**
     * All visible pages are children of a div named 'top'.
      */
    static allPages: FullPage[] = [];  // should be the same as the children of top 'div'

    /**
     * Creates a page which will be used to display some rendering.
     * @param {string} title         Title to use for page.
     * @param {ViewKind} viewKind    Kind of view that is being displayed.
     * @param {FullPage} sourcePage  Page which initiated the creation of this one.
     */
    public constructor(title: string, viewKind: ViewKind, sourcePage: FullPage) {
        this.pageId = FullPage.pageCounter++;
        this.console = new ConsoleDisplay();
        this.progressManager = new ProgressManager();
        this.dataDisplay = new DataDisplay();

        this.pageTopLevel = document.createElement("div");
        this.pageTopLevel.className = "hillviewPage";
        this.pageTopLevel.id = "hillviewPage" + this.pageId.toString();
        this.bottomContainer = document.createElement("div");

        let titleRow = document.createElement("div");
        titleRow.style.display = "flex";
        titleRow.style.width = "100%";
        titleRow.style.flexDirection = "row";
        titleRow.style.flexWrap = "false";
        titleRow.style.alignItems = "center";
        this.pageTopLevel.appendChild(titleRow);
        this.menuSlot = document.createElement("div");
        this.addCell(titleRow, this.menuSlot, true);

        let help = document.createElement("div");
        help.onclick = () => this.openInNewTab(this.helpUrl(viewKind));
        help.textContent = "help";
        help.className = "external-link";
        help.style.cursor = "help";
        help.title = "Open help documentation related to this view.";
        this.addCell(titleRow, help, true);

        let h1 = document.createElement("h1");
        h1.innerHTML = title;
        h1.style.textOverflow = "ellipsis";
        h1.style.textAlign = "center";
        h1.style.margin = "0";
        this.addCell(titleRow, h1, false);

        if (sourcePage != null) {
            h1.innerHTML += " from ";
            let refLink = this.pageReference(sourcePage.pageId);
            refLink.title = "View which produced this one.";
            h1.appendChild(refLink);
        }

        let pageId = document.createElement("span");
        pageId.textContent = "[" + this.pageId + "]";
        pageId.title = "Unique number of this view.";
        this.addCell(titleRow, pageId, true);

        let close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.onclick = (e) => this.remove();
        close.title = "Close this view.";
        this.addCell(titleRow, close, true);

        this.pageTopLevel.appendChild(this.dataDisplay.getHTMLRepresentation());
        this.pageTopLevel.appendChild(this.bottomContainer);

        this.bottomContainer.appendChild(this.progressManager.getHTMLRepresentation());
        this.bottomContainer.appendChild(this.console.getHTMLRepresentation());
    }

    openInNewTab(url: string) {
        let win = window.open(url, "_blank");
        win.focus();
    }

    /**
     * Returns a URL to a section in the user manual.
     * @param {ViewKind} viewKind  Kind of view that help is sought for.
     */
    helpUrl(viewKind: ViewKind): string {
        let ref = helpUrl[viewKind];
        return "https://github.com/vmware/hillview/blob/master/docs/userManual.md#" + ref;
    }

    /**
     * @eturns An html string that represents a reference to the specified page.
     */
    pageReference(pageId: number): HTMLElement {
        let refLink = document.createElement("a");
        refLink.href = "#";
        refLink.textContent = "[" + pageId + "]";
        refLink.onclick = () => this.navigateToPage(pageId);
        return refLink;
    }

    setMenu(c: TopMenu): void {
        removeAllChildren(this.menuSlot);
        this.menuSlot.appendChild(c.getHTMLRepresentation());
    }

    addCell(row: HTMLElement, c: HTMLElement, minSize: boolean): void {
        if (minSize != null && minSize)
            c.style.flexGrow = "1";
        else
            c.style.flexGrow = "100";
        row.appendChild(c);
    }

    protected navigateToPage(pageId: number): boolean {
        let found = false;
        for (let p of FullPage.allPages) {
            if (p.pageId == pageId) {
                p.scrollIntoView();
                found = true;
            }
        }
        // return false to prevent the url from being followed.
        if (!found)
            this.reportError("Page [" + pageId + "] no longer exists");
        return false;
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

    public setDataView(hdv: IDataView): void {
        this.dataDisplay.setDataView(hdv);
    }

    public reportError(error: string) {
        this.getErrorReporter().clear();
        this.getErrorReporter().reportError(error);
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

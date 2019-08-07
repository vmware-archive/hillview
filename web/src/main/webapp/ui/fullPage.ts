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

import {DatasetView} from "../datasetView";
import {makeMissing, makeSpan, openInNewTab, significantDigitsHtml} from "../util";
import {IDataView} from "./dataview";
import {ConsoleDisplay, ErrorReporter} from "./errReporter";
import {TopMenu} from "./menu";
import {ProgressManager} from "./progress";
import {HtmlString, IHtmlElement, removeAllChildren, SpecialChars, ViewKind} from "./ui";
import {helpUrl} from "./helpUrl";
import {BigTableView} from "../tableTarget";
import {CombineOperators} from "../javaBridge";

const minus = "&#8722;";
const plus = "+";

export class PageTitle {
    public static readonly missingFormat = "%m";

    /**
     * A title is described by a format string.
     * @param format  Format string, described below.
     * %m represents a "missing" value.
     * %p(n) represents a link to page numbered n.
     */
    constructor(public readonly format: string) {}

    public getHTMLRepresentation(parentPage: FullPage): HTMLElement {
        const result = document.createElement("span");
        let remaining = this.format;
        while (true) {
            const next = remaining.indexOf("%");
            if (next === -1 || next === remaining.length - 1) {
                result.appendChild(makeSpan(remaining));
                break;
            }
            if (remaining[next + 1] === "p") {
                if (next > 0)
                    result.appendChild(makeSpan(remaining.substr(0, next - 1)));
                remaining = remaining.substr(next + 1);  // skip %p
                const numre = /\((\d+)\)(.*)/;
                const matches = numre.exec(remaining);
                if (matches) {
                    const num = parseInt(matches[1], 10);
                    result.appendChild(parentPage.pageReference(num));
                    remaining = matches[2];
                }
            } else if (remaining[next + 1] === "m") {
                if (next > 0)
                    result.appendChild(makeSpan(remaining.substr(0, next - 1)));
                result.appendChild(makeMissing());
                remaining = remaining.substr(next + 2);
            } else {
                result.appendChild(makeSpan(remaining));
                break;
            }
        }
        return result;
    }
}

// Kind of data that is being dragged
export type DragEventKind = "Title" | "XAxis" | "YAxis" | "GAxis";

/**
 * A FullPage is the main unit of display in Hillview, storing on rendering.
 * The page layout is as follows:
 * -------------------------------------------------
 * | menu            #. Title       X Y G ^ v ? - x|  titleRow
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
    public dataView: IDataView | null;
    public bottomContainer: HTMLElement;
    public progressManager: ProgressManager;
    protected console: ConsoleDisplay;
    public pageTopLevel: HTMLElement;
    private readonly menuSlot: HTMLElement;
    private readonly h2: HTMLElement;
    private minimized: boolean;
    private readonly displayHolder: HTMLElement;
    protected titleRow: HTMLDivElement;
    protected help: HTMLElement;
    protected xDrag: HTMLElement;
    protected yDrag: HTMLElement;
    protected gDrag: HTMLElement;
    // These functions are registered to handle drop events.
    // Each drop event has a text payload and starts with a prefix.
    // The functions are each registered for a prefix.
    protected dropHandler: Map<string, (s: string) => void>;

    /**
     * Creates a page which will be used to display some rendering.
     * @param pageId      Page number within dataset.
     * @param title       Title to use for page.
     * @param sourcePageId Id of page which initiated the creation of this one.
     *                     Null if this is the first page in a dataset.
     * @param dataset     Parent dataset; only null for the toplevel menu.
     */
    public constructor(public readonly pageId: number,
                       public readonly title: PageTitle,
                       public readonly sourcePageId: number | null,
                       public readonly dataset: DatasetView) {
        this.dataView = null;
        this.console = new ConsoleDisplay();
        this.progressManager = new ProgressManager();
        this.minimized = false;
        this.dropHandler = new Map<string, (s: string) => void>();

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

        const h2 = document.createElement("h2");
        if (this.title != null) {
            const titleStart = (this.pageId > 0 ? (this.pageId.toString() + ". ") : "");
            h2.appendChild(makeSpan(titleStart));
            h2.appendChild(this.title.getHTMLRepresentation(this));
            h2.style.cursor = "grab";
            h2.draggable = true;
            h2.ondragstart = (e) => this.setDragPayload(e, "Title");
            h2.title = "View title.  Can be dragged to replace data in another view.";
        }
        h2.style.textOverflow = "ellipsis";
        h2.style.textAlign = "center";
        h2.style.margin = "0";
        this.h2 = h2;
        this.addCell(h2, false);
        this.registerDropHandler("Title", (s) => this.dropCombine(s));

        if (sourcePageId != null) {
            h2.appendChild(makeSpan(" from "));
            const refLink = this.pageReference(sourcePageId);
            refLink.title = "View which produced this one.";
            h2.appendChild(refLink);
        }

        if (this.dataset != null) {
            this.xDrag = makeSpan("X");
            this.xDrag.title = "Drag this to copy the X axis to another chart";
            this.xDrag.className = "axisbox";
            this.addCell(this.xDrag, true);
            this.xDrag.draggable = true;
            this.xDrag.style.visibility = "hidden";
            this.xDrag.ondragstart = (e) => this.setDragPayload(e, "XAxis");

            this.yDrag = makeSpan("Y");
            this.yDrag.title = "Drag this to copy the Y axis to another chart";
            this.yDrag.className = "axisbox";
            this.addCell(this.yDrag, true);
            this.yDrag.draggable = true;
            this.yDrag.style.visibility = "hidden";
            this.yDrag.ondragstart = (e) => this.setDragPayload(e, "YAxis");

            this.gDrag = makeSpan("G");
            this.gDrag.title = "Drag this to copy the group-by axis to another chart";
            this.gDrag.className = "axisbox";
            this.addCell(this.gDrag, true);
            this.gDrag.draggable = true;
            this.gDrag.style.visibility = "hidden";
            this.gDrag.ondragstart = (e) => this.setDragPayload(e, "GAxis");

            const moveUp = document.createElement("div");
            moveUp.innerHTML = SpecialChars.upArrow;
            moveUp.title = "Move window up.";
            moveUp.onclick = () => this.dataset.shift(this, true);
            moveUp.style.cursor = "pointer";
            this.addCell(moveUp, true);

            const moveDown = document.createElement("div");
            moveDown.innerHTML = SpecialChars.downArrow;
            moveDown.title = "Move window down.";
            moveDown.onclick = () => this.dataset.shift(this, false);
            moveDown.style.cursor = "pointer";
            this.addCell(moveDown, true);
        }

        this.help = document.createElement("button");
        this.help.textContent = "?";
        this.help.className = "help";
        this.help.title = "Open help documentation related to this view.";
        this.addCell(this.help, true);

        if (this.dataset != null) {
            // The load menu does not have these decorative elements
            const minimize = document.createElement("span");
            minimize.className = "minimize";
            minimize.innerHTML = minus;
            minimize.onclick = () => this.minimize(minimize);
            minimize.title = "Minimize this view.";
            this.addCell(minimize, true);

            const close = document.createElement("span");
            close.className = "close";
            close.innerHTML = "&times;";
            close.onclick = () => this.dataset.remove(this);
            close.title = "Close this view.";
            this.addCell(close, true);
        }

        this.displayHolder = document.createElement("div");
        this.displayHolder.ondragover = (event) => event.preventDefault();
        this.displayHolder.ondrop = (event) => this.dropped(event);
        this.pageTopLevel.appendChild(this.displayHolder);
        this.pageTopLevel.appendChild(this.bottomContainer);

        this.bottomContainer.appendChild(this.progressManager.getHTMLRepresentation());
        this.bottomContainer.appendChild(this.console.getHTMLRepresentation());
    }

    /**
     * Register a function to be called when a drop event has happened.
     * The drop event payload is a text.
     * @param prefix   Prefix of the text, separated by colon from the rest.
     * @param handler  A function that receives the rest of the text, after the colon.
     */
    public registerDropHandler(prefix: DragEventKind, handler: (data: string) => void): void {
        this.dropHandler.set(prefix, handler);
    }

    protected dropped(e: DragEvent): void {
        e.preventDefault();
        const payload = e.dataTransfer.getData("text");
        const parts = payload.split(":", 2);
        console.assert(parts.length === 2);
        const handler = this.dropHandler.get(parts[0]);
        console.assert(handler != null);
        handler(parts[1]);
    }

    /**
     * Set the drag event payload to be the current page id.
     * @param e    DragEvent that is being modified.
     * @param type Type of payload carried in the drag event.
     */
    public setDragPayload(e: DragEvent, type: DragEventKind): void {
        e.dataTransfer.setData("text", type + ":" + this.pageId.toString());
    }

    /**
     * A a drop handler that will handle a "Title" drop event.  It performs
     * a combine operation with replacement - replacing the current dataset
     * with the one dropped.  The payload is the number of the page holding the
     * source dataset.
     */
    protected dropCombine(pageId: string): void {
        const view = this.dataView as BigTableView;
        if (view == null)
            return;
        this.dataset.select(Number(pageId));
        view.combine(CombineOperators.Replace);
    }

    public setViewKind(viewKind: ViewKind): void {
        this.help.onclick = () => openInNewTab(FullPage.helpUrl(viewKind));
    }

    public getTitleElement(): HTMLElement {
        return this.h2;
    }

    /**
     * Returns a URL to a section in the user manual.
     * @param {ViewKind} viewKind  Kind of view that help is sought for.
     */
    public static helpUrl(viewKind: ViewKind): string {
        let ref = helpUrl[viewKind];
        // strip parentheses from front and back
        ref = ref.replace(/^\(/, "").replace(/\)$/, "");
        return "https://github.com/vmware/hillview/blob/master/docs/userManual.md" + ref;
    }

    /**
     * @eturns An html string that represents a reference to the specified page.
     */
    public pageReference(pageId: number): HTMLElement {
        const refLink = document.createElement("a");
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

    public setMenu(c: TopMenu): void {
        removeAllChildren(this.menuSlot);
        this.menuSlot.appendChild(c.getHTMLRepresentation());
    }

    public addCell(c: HTMLElement, minSize: boolean): void {
        if (!minSize)
            c.style.flexGrow = "100";
        this.titleRow.appendChild(c);
    }

    public minimize(span: HTMLElement): void {
        if (this.minimized) {
            if (this.dataView != null)
                this.displayHolder.appendChild(this.dataView.getHTMLRepresentation());
            this.minimized = false;
            span.innerHTML = minus;
        } else {
            removeAllChildren(this.displayHolder);
            this.minimized = true;
            span.innerHTML = plus;
        }
    }

    public onResize(): void {
        if (this.dataView != null)
            this.dataView.resize();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.pageTopLevel;
    }

    public getErrorReporter(): ErrorReporter {
        return this.console;
    }

    public setDataView(hdv: IDataView | null): void {
        this.dataView = hdv;
        removeAllChildren(this.displayHolder);
        if (hdv != null) {
            this.displayHolder.appendChild(hdv.getHTMLRepresentation());
            this.setViewKind(hdv.viewKind);
            switch (this.dataView.viewKind) {
                case "Histogram":
                case "2DHistogram":
                case "Heatmap":
                    this.xDrag.style.visibility = "visible";
                    this.yDrag.style.visibility = "visible";
                    break;
                case "TrellisHistogram":
                case "Trellis2DHistogram":
                case "TrellisHeatmap":
                    this.xDrag.style.visibility = "visible";
                    this.yDrag.style.visibility = "visible";
                    this.gDrag.style.visibility = "visible";
                    break;
                case "Table":
                case "HeavyHitters":
                case "Schema":
                case "Load":
                case "SVD Spectrum":
                case "LogFileView":
                    break;
            }
        }
    }

    public getDataView(): IDataView | null {
        return this.dataView;
    }

    public reportError(error: string): void {
        this.getErrorReporter().clear();
        this.getErrorReporter().reportError(error);
    }

    public clearError(): void {
        this.getErrorReporter().clear();
    }

    public reportTime(timeInMs: number): void {
        this.getErrorReporter().reportFormattedError(
            new HtmlString("Operation took ")
                .append(significantDigitsHtml(timeInMs / 1000))
                .appendSafeString(" seconds"));
    }

    public getWidthInPixels(): number {
        return Math.floor(this.pageTopLevel.getBoundingClientRect().width);
    }

    public scrollIntoView(): void {
        this.getHTMLRepresentation().scrollIntoView( { block: "end", behavior: "smooth" } );
    }
}

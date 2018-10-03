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
import {openInNewTab, significantDigitsHtml} from "../util";
import {IDataView} from "./dataview";
import {ConsoleDisplay, ErrorReporter} from "./errReporter";
import {TopMenu} from "./menu";
import {ProgressManager} from "./progress";
import {HtmlString, IHtmlElement, removeAllChildren, ViewKind} from "./ui";
import {helpUrl} from "./helpUrl";

const minus = "&#8722;";
const plus = "+";

/**
 * A FullPage is the main unit of display in Hillview, storing on rendering.
 * The page layout is as follows:
 * -------------------------------------------------
 * | menu            #. Title                 ? - x|  titleRow
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
    public dataView: IDataView;
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

    /**
     * Creates a page which will be used to display some rendering.
     * @param pageId      Page number within dataset.
     * @param title       Title to use for page.
     * @param sourcePageId Id of page which initiated the creation of this one.
     *                     Null if this is the first page in a dataset.
     * @param dataset     Parent dataset; only null for the toplevel menu.
     */
    public constructor(public readonly pageId: number,
                       public readonly title: string,
                       public readonly sourcePageId: number | null,
                       public readonly dataset: DatasetView) {
        this.console = new ConsoleDisplay();
        this.progressManager = new ProgressManager();
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

        const h2 = document.createElement("h2");
        if (title != null)
            h2.innerHTML = (this.pageId > 0 ? (this.pageId.toString() + ". ") : "") + title;
        h2.style.textOverflow = "ellipsis";
        h2.style.textAlign = "center";
        h2.style.margin = "0";
        this.h2 = h2;
        this.addCell(h2, false);

        if (sourcePageId != null) {
            h2.innerHTML += " from ";
            const refLink = this.pageReference(sourcePageId);
            refLink.title = "View which produced this one.";
            h2.appendChild(refLink);
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

            /*
            let pageIdSpan = document.createElement("span");
            pageIdSpan.textContent = "[" + this.pageId + "]";
            pageIdSpan.title = "Unique number of this view.";
            pageIdSpan.draggable = true;
            pageIdSpan.ondragstart = (e) => e.dataTransfer.setData("text", this.pageId.toString());
            this.addCell(pageIdSpan, true);
            */

            const close = document.createElement("span");
            close.className = "close";
            close.innerHTML = "&times;";
            close.onclick = () => this.dataset.remove(this);
            close.title = "Close this view.";
            this.addCell(close, true);
        }

        this.displayHolder = document.createElement("div");
        this.pageTopLevel.appendChild(this.displayHolder);
        this.pageTopLevel.appendChild(this.bottomContainer);

        this.bottomContainer.appendChild(this.progressManager.getHTMLRepresentation());
        this.bottomContainer.appendChild(this.console.getHTMLRepresentation());
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
        this.dataView.resize();
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.pageTopLevel;
    }

    public getErrorReporter(): ErrorReporter {
        return this.console;
    }

    public setDataView(hdv: IDataView): void {
        this.dataView = hdv;
        removeAllChildren(this.displayHolder);
        this.displayHolder.appendChild(hdv.getHTMLRepresentation());
        this.setViewKind(hdv.viewKind);
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
                .appendString(" seconds"));
    }

    public getWidthInPixels(): number {
        return this.pageTopLevel.getBoundingClientRect().width;
    }

    public scrollIntoView(): void {
        this.getHTMLRepresentation().scrollIntoView( { block: "end", behavior: "smooth" } );
    }
}

/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {IHtmlElement} from "./ui";
import {px} from "../util";
import {drag as d3drag} from "d3-drag";
import {select as d3select} from "d3-selection";
import {mouse as d3mouse} from "d3-selection";

/**
 * A table-like view with resizable columns.
 */
export class Grid implements IHtmlElement {
    protected htmlTable: HTMLTableElement;
    protected tHead: HTMLTableSectionElement;
    protected tBody: HTMLTableSectionElement;
    protected topLevel: HTMLElement;
    protected rowCount: number;
    protected columnCount: number;
    protected thr: HTMLTableRowElement;
    protected lastRow: HTMLTableRowElement;
    protected currentColumn: number;
    protected readonly minColumnWidth: number = 20;
    protected currentWidth: number;
    protected resizable: boolean[];  // True if a column is resizable
    private colWidths: Map<string, number>;  // Saved column widths

    /*
        This builds a table with this structure:
        <table style="width:253px" id="table">
            <tr>
                <td style="width:200px" id="col1">
                    <div class="resizable">
                        <div class="truncated">Text to display</div>
                        <div class="handle" data-col="col1"></div>
                    </div>
                </td>
            </tr>
            ...
        </table>
     */
    public constructor(public readonly defaultColumnWidth) {
        this.topLevel = document.createElement("div");
        this.topLevel.style.overflowX = "scroll";
        this.topLevel.style.width = "100%";
        this.topLevel.style.position = "relative";

        this.htmlTable = document.createElement("table");
        this.htmlTable.className = "tableView";
        this.lastRow = null;
        this.tBody = null;
        this.topLevel.appendChild(this.htmlTable);
    }

    public setSize(rows: number, columns: number): void {
        this.rowCount = rows;
        this.columnCount = columns;
    }

    private createResizable(fixedWidth: boolean): HTMLElement {
        const resizable = document.createElement("div");
        resizable.className = "resizable";

        const truncated = document.createElement("div");
        resizable.appendChild(truncated);
        truncated.className = "truncated";

        if (!fixedWidth) {
            const handle = document.createElement("div");
            resizable.appendChild(handle);
            handle.className = "handle";
            handle.setAttribute("data-col", this.currentColumn.toString());
            const drag = d3drag()
                .on("drag", () => this.drag(handle))
                .on("start", () => this.dragStarted(handle));
            d3select(handle).call(drag);
            handle.ondblclick = () => this.resize(handle);
        }
        return resizable;
    }

    private resize(handle: HTMLElement): void {
        const cls = handle.getAttribute("data-col");
        const header = this.getHeader(Number(cls));
        const origColWidth = header.offsetWidth;
        // Changing these widths sets all columns to their natural width.
        header.style.width = "";
        const origWidth = this.htmlTable.offsetWidth;
        this.htmlTable.style.width = "";
        // All columns have grown to their natural size.  Measure
        // the new one and set the sizes back.
        const currentColWidth = header.offsetWidth;
        header.style.width = currentColWidth + "px";
        this.htmlTable.style.width = origWidth + currentColWidth - origColWidth + "px";
    }

    private static getDataCell(resizable: HTMLElement): HTMLElement {
        return resizable.getElementsByClassName("truncated")[0] as HTMLElement;
    }

    /**
     * @param width   in pixels. if not zero the column is considered fixed width.
     * @param colName Column to add.
     * @param forgetWidth   if true we do not reuse the old width.
     * @param className     Class to add to list of classes for header.
     */
    public addHeader(width: number, colName: string,
                     forgetWidth: boolean, className: string): HTMLElement {
        const td = document.createElement("td");
        td.classList.add("header");
        if (className != null)
            td.classList.add(className);
        this.thr.appendChild(td);

        td.classList.add("preventselection");
        td.setAttribute("data-colname", colName);
        let useWidth = width;
        if (this.colWidths != null && !forgetWidth &&
            this.colWidths.has(colName)) {
            useWidth = this.colWidths.get(colName);
        }
        const w = (useWidth !== 0) ? useWidth : this.defaultColumnWidth;
        td.style.width = px(w);

        const resizable = this.createResizable(width !== 0);
        td.appendChild(resizable);
        this.currentColumn++;
        this.currentWidth += w + 1;  // 1 for the borders
        return Grid.getDataCell(resizable);
    }

    // Used when dragging
    protected startPosition: number;
    protected originalWidth: number;
    protected startTableWidth: number;

    private drag(node: HTMLElement): void {
        const x = d3mouse(this.topLevel)[0];
        const delta = this.startPosition - x;

        let width = this.originalWidth - delta;
        width = Math.max(this.minColumnWidth, width);

        const cls = node.getAttribute("data-col");
        const header = this.getHeader(Number(cls));
        header.style.width = px(width);
        const tw = this.startTableWidth - delta;
        this.htmlTable.style.width = px(tw);
    }

    private dragStarted(node: HTMLElement): void {
        this.startPosition = d3mouse(this.topLevel)[0];
        this.originalWidth = node.parentElement.offsetWidth;
        this.startTableWidth = this.htmlTable.offsetWidth;
    }

    public newRow(): void {
        this.lastRow = this.tBody.insertRow();
        this.currentColumn = 0;
    }

    public newCell(cls: string): HTMLElement {
        const cell = this.lastRow.insertCell();
        cell.classList.add(cls);
        const resizable = this.createResizable(this.resizable[this.currentColumn]);
        cell.appendChild(resizable);
        this.currentColumn++;
        return Grid.getDataCell(resizable);
    }

    public getHeader(colIndex: number): HTMLElement {
        return this.thr.cells[colIndex];
    }

    public prepareForUpdate(): void {
        if (this.tHead != null) {
            this.colWidths = new Map<string, number>();
            for (let i = 0; i < this.thr.childElementCount; i++) {
                const th = this.thr.childNodes[i] as HTMLElement;
                const colName = th.getAttribute("data-colname");
                if (colName != null) {
                    // minus one for the borders
                    this.colWidths.set(colName, th.offsetWidth - 1);
                }
            }
            this.tHead.remove();
        }
        if (this.tBody != null)
            this.tBody.remove();
        this.resizable = [];
        this.tBody = this.htmlTable.createTBody();
        this.tHead = this.htmlTable.createTHead();
        this.thr = this.tHead.appendChild(document.createElement("tr"));
        this.currentColumn = 0;
        this.currentWidth = 0;
    }

    public updateCompleted(): void {
        this.htmlTable.style.width = px(this.currentWidth);
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}

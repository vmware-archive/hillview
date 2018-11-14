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

import {default as ColumnResizer} from "../ColumnResizer";
import {IHtmlElement} from "./ui";

/**
 * A table-like view.
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

    // Support for column resizing
    protected resizer: ColumnResizer;
    // See the documentation for ColumnResizer: https://github.com/MonsantoCo/column-resizer
    protected readonly draggingProperties = {
        disable: false,
        resizeMode: "overflow",
        liveDrag: true,
        draggingClass: "dragging",
        disabledColumns: [0, 1],
        removePadding: false,
        postbackSafe: true,
        partialRefresh: true,
        minWidth: 10
    };

    public constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.style.overflowX = "scroll";
        this.topLevel.style.width = "100%";
        this.topLevel.style.position = "relative";

        this.resizer = null;
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

    public addHeader(title: string): HTMLElement {
        const th = document.createElement("th");
        th.classList.add("noselect");
        th.style.overflow = "hidden";
        th.title = title;
        this.thr.appendChild(th);
        return th;
    }

    public newRow(): void {
        this.lastRow = this.tBody.insertRow();
    }

    public newCell(): HTMLElement {
        const cell = this.lastRow.insertCell();
        cell.style.overflow = "hidden";
        return cell;
    }

    public getHeader(colIndex: number): HTMLTableHeaderCellElement {
        return this.thr.cells[colIndex];
    }

    public prepareForUpdate(): void {
        if (this.resizer != null)
            this.resizer.reset({ disable: true });
        if (this.tHead != null)
            this.tHead.remove();
        if (this.tBody != null)
            this.tBody.remove();
        this.tBody = this.htmlTable.createTBody();
        this.tHead = this.htmlTable.createTHead();
        this.thr = this.tHead.appendChild(document.createElement("tr"));
    }

    public updateCompleted(): void {
        if (this.resizer == null) {
            // This does not seem to work well.
            // this.resizer = new ColumnResizer(this.htmlTable, this.draggingProperties);
        } else {
            this.resizer.reset(this.draggingProperties);
        }
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }
}

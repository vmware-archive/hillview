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

import {PlottingSurface} from "./plottingSurface";
import {SelectionStateMachine} from "./selectionStateMachine";
import {IHtmlElement} from "./ui";
import {px, truncate} from "../util";

/**
 * A TabularDisplay is a visual representation that uses an HTML table to display
 * some data.  This is used to display various small datasets, such as the
 * heavy hitters or the schema of a "real" remote table.
 */
export class TabularDisplay implements IHtmlElement {
    public topLevel: HTMLElement;
    public table: HTMLTableElement | null;
    public tbody: HTMLTableSectionElement;
    public selectedRows: SelectionStateMachine;
    public rowCount: number;
    public columnCount: number;
    public colHeaderMap: Map<string, HTMLElement>;
    public rows: HTMLTableRowElement[];

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.classList.add("tabularDisplay");
        this.topLevel.style.maxHeight = px(PlottingSurface.canvasHeight);
        this.topLevel.style.overflowY = "scroll";  // This should be auto, but it looks bad on Mozilla
        this.table = null;
        this.clear();
    }

    public scrollPosition(): number {
        return this.topLevel.scrollTop;
    }

    public setScrollPosition(pos: number): void {
        this.topLevel.scrollTop = pos;
    }

    public clear(): void {
        if (this.table != null)
            this.topLevel.removeChild(this.table);
        this.table = document.createElement("table");
        this.topLevel.appendChild(this.table);
        this.tbody = this.table.createTBody();
        this.selectedRows = new SelectionStateMachine();
        this.rowCount = 0;
        this.columnCount = 0;
        this.colHeaderMap = new Map<string, HTMLElement>();
        this.rows = [];
    }

    public getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    /**
     * Create columns with the specified names.
     */
    public setColumns(colNames: string[], toolTips: string[]): void {
        const tHead = this.table!.createTHead();
        const thr = tHead.appendChild(document.createElement("tr"));
        let index = 0;
        for (const c of colNames) {
            const tip = toolTips[index++];
            const thd = document.createElement("th");
            thd.title = tip;
            thd.textContent = c;
            thr.appendChild(thd);
            thd.classList.add("preventselection");
            this.columnCount++;
            this.colHeaderMap.set(c, thd);
        }
    }

    public insertRow(canClick: boolean = true): HTMLTableRowElement {
        const trow = this.tbody.insertRow();
        const rowNo = this.rowCount;
        trow.id = "row" + this.rowCount;
        if (canClick)
            trow.onclick = (e) => this.clickRowIndex(rowNo, e);
        this.rows.push(trow);
        this.rowCount++;
        return trow;
    }

    private static addRowCell(trow: HTMLTableRowElement): HTMLTableCellElement {
        const cell = trow.insertCell(trow.cells.length);
        cell.style.textAlign = "right";
        cell.classList.add("preventselection");
        return cell;
    }

    public addRightClickHandler(colName: string, handler: (e: Event) => void): void {
        this.colHeaderMap.get(colName).oncontextmenu = handler;
    }

    /**
     * Add a row of values; these are set as the text values of the cells.
     */
    public addRow(data: string[], canClick: boolean = true): HTMLTableRowElement {
        const trow = this.insertRow(canClick);
        for (const d of data) {
            const cell = TabularDisplay.addRowCell(trow);
            cell.textContent = truncate(d, 50);
            cell.title = d;
        }
        return trow;
    }

    public addFooter(): void {
        const footer = this.tbody.insertRow();
        const cell = footer.insertCell(0);
        cell.colSpan = this.columnCount;
        cell.className = "footer";
    }

    /**
     * Add a row of values; these are set as the dom children of the table cells
     */
    public addElementRow(data: Element[], canClick: boolean = true): HTMLTableRowElement  {
        const trow = this.insertRow(canClick);
        for (const d of data) {
            const cell = TabularDisplay.addRowCell(trow);
            cell.appendChild(d);
            d.classList.add("preventselection");
        }
        return trow;
    }

    public excludeRow(val: number): void {
        this.selectedRows.exclude(val);
    }

    public getSelectedRows(): Set<number> {
        return this.selectedRows.getStates();
    }

    /**
     * This method handles the transitions in the set of selected rows resulting from mouse clicks,
     * combined with various kinds of key selections
     */
    private clickRowIndex(rowIndex: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.button === 2) { // right button
            if (!this.selectedRows.has(rowIndex))// Add the row if not already present.
                this.selectedRows.changeState("Ctrl", rowIndex);
        } else {
            if (e.ctrlKey || e.metaKey) {
                this.selectedRows.changeState("Ctrl", rowIndex);
            } else if (e.shiftKey) {
                this.selectedRows.changeState("Shift", rowIndex);
            } else
                this.selectedRows.changeState("NoKey", rowIndex);
        }
        this.highlightSelectedRows();
    }

    public clickRow(htmlRow: HTMLTableRowElement, e: MouseEvent): void {
        const i: number = this.rows.indexOf(htmlRow);
        if (i !== -1)
           this.clickRowIndex(i, e);
        return;
    }

    public highlightSelectedRows(): void {
        for (let i = 0; i < this.rowCount; i++) {
            const rowi = this.rows[i];
            if (this.selectedRows.has(i))
                rowi.classList.add("selected");
            else
                rowi.classList.remove("selected");
        }
    }
}

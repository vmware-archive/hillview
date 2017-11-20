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

import {IHtmlElement, Resolution} from "./ui";
import {SelectionStateMachine} from "./selectionStateMachine";

/**
 * A TabularDisplay is a visual representation that uses an HTML table to display
 * some data.  This is used to display various small datasets, such as the
 * heavy hitters or the schema of a "real" remote table.
 */
export class TabularDisplay implements IHtmlElement {
    topLevel: HTMLElement;
    table: HTMLTableElement;
    tbody: HTMLTableSectionElement;
    selectedRows: SelectionStateMachine;
    rowCount: number;
    columnCount: number;
    rows: HTMLTableRowElement[];

    constructor() {
        this.topLevel = document.createElement("div");
        this.topLevel.style.maxHeight = Resolution.canvasHeight.toString() + "px";
        this.topLevel.style.overflowY = "auto";
        this.topLevel.style.display =  "inline-block";

        this.table = document.createElement("table");
        this.topLevel.appendChild(this.table);
        this.tbody = this.table.createTBody();
        this.selectedRows = new SelectionStateMachine();
        this.rowCount = 0;
        this.columnCount = 0;
        this.rows = [];
    }

    getHTMLRepresentation(): HTMLElement {
        return this.topLevel;
    }

    /**
     * Create columns.  The column names are used as the innerHTML of the cells.
     */
    public setColumns(colNames: string[], toolTips: string[]): void {
        let tHead = this.table.createTHead();
        let thr = tHead.appendChild(document.createElement("tr"));
        let index = 0;
        for (let c of colNames) {
            let tip = toolTips[index++];
            let thd = document.createElement("th");
            thd.title = tip;
            thd.innerHTML = c;
            thr.appendChild(thd);
            thd.classList.add("noselect");
            this.columnCount++;
        }
    }

    /**
     * Add a row of values; these are set as the innerHTML values of the cells.
     */
    public addRow(data: string[]): void {
        let trow = this.tbody.insertRow();
        this.rows.push(trow);
        let rowCells = [];
        let index = 0;
        let rowNo = this.rowCount;
        for (let d of data) {
            let cell = trow.insertCell(index++);
            cell.style.textAlign = "right";
            cell.innerHTML = d;
            cell.classList.add("noselect");
            cell.onclick = e => this.rowClick(rowNo, e);
            rowCells.push(cell);
        }
        this.rowCount++;
    }

    public addFooter() {
        let footer = this.tbody.insertRow();
        let cell = footer.insertCell(0);
        cell.colSpan = this.columnCount;
        cell.className = "footer";
    }

    /**
     * Add a row of values; these are set as the dom children of the table cells
     */
    public addElementRow(data: Element[]): void {
        let trow = this.tbody.insertRow();
        this.rows.push(trow);
        let rowCells = [];
        let index = 0;
        let rowNo = this.rowCount;
        for (let d of data) {
            let cell = trow.insertCell(index++);
            cell.style.textAlign = "right";
            cell.appendChild(d);
            cell.classList.add("noselect");
            d.classList.add("noselect");
            cell.onclick = e => this.rowClick(rowNo, e);
            rowCells.push(cell);
        }
        this.rowCount++;
    }

    public getSelectedRows(): Set<number> {
        return this.selectedRows.getStates();
    }

    /**
     * This method handles the transitions in the set of selected rows resulting from mouse clicks,
     * combined with various kinds of key selections
     */
    private rowClick(rowIndex: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey) {
            this.selectedRows.changeState("Ctrl", rowIndex);
        } else if (e.shiftKey) {
            this.selectedRows.changeState("Shift", rowIndex);
        } else
            this.selectedRows.changeState("NoKey", rowIndex);
        this.highlightSelectedRows();
    }

    private highlightSelectedRows(): void {
        for (let i = 0; i < this.rowCount; i++) {
            let rowi = this.rows[i];
            if (this.selectedRows.has(i))
                rowi.classList.add("selected");
            else
                rowi.classList.remove("selected");
        }
    }
}
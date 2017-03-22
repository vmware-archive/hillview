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

import {IHtmlElement, ScrollBar, Menu, Renderer, FullPage, HieroDataView} from "./ui";
import {RemoteObject, PartialResult, RpcReceiver, ICancellable} from "./rpc";
import Rx = require('rx');
import {RangeCollector} from "./histogram";

// These classes are direct counterparts to server-side Java classes
// with the same names.  JSON serialization
// of the Java classes produces JSON that can be directly cast
// into these interfaces.
export enum ContentsKind {
    Category,
    Json,
    String,
    Integer,
    Double,
    Date,
    Interval
}

export interface IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;
}

// Direct counterpart to Java class
export class ColumnDescription implements IColumnDescription {
    readonly kind: ContentsKind;
    readonly name: string;
    readonly allowMissing: boolean;

    constructor(v : IColumnDescription) {
        this.kind = v.kind;
        this.name = v.name;
        this.allowMissing = v.allowMissing;
    }
}

// Direct counterpart to Java class
export interface Schema {
    [index: number] : IColumnDescription;
    length: number;
}

export interface RowView {
    count: number;
    values: any[];
}

// Direct counterpart to Java class
export interface ColumnSortOrientation {
    columnDescription: IColumnDescription;
    isAscending: boolean;
}

// Direct counterpart to Java class
class RecordOrder {
    constructor(public sortOrientationList: Array<ColumnSortOrientation>) {}
    public length(): number { return this.sortOrientationList.length; }
    public get(i: number): ColumnSortOrientation { return this.sortOrientationList[i]; }

    // Find the index of a specific column; return -1 if columns is not in the sort order
    public find(col: string): number {
        for (let i = 0; i < this.length(); i++)
            if (this.sortOrientationList[i].columnDescription.name == col)
                return i;
        return -1;
    }
    public hide(col: string): void {
        let index = this.find(col);
        if (index == -1)
            // already hidden
            return;
        this.sortOrientationList.splice(index, 1);
    }
    public show(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.splice(0, 0, cso);
    }
    public clone() : RecordOrder {
        return new RecordOrder(this.sortOrientationList.slice(0));
    }
}

export class TableDataView {
    public schema?: Schema;
    // Total number of rows in the complete table
    public rowCount: number;
    public startPosition?: number;
    public rows?: RowView[];
}

/* Example table view:
-------------------------------------------
| pos | count | col0 v1 | col1 ^0 | col2 |
-------------------------------------------
| 10  |     3 | Mike    |       0 |      |
 ------------------------------------------
 | 13 |     6 | Jon     |       1 |      |
 ------------------------------------------
 */

export class TableView extends RemoteObject
    implements IHtmlElement, HieroDataView {
    // Data view part: received from remote site
    protected schema?: Schema;
    // Logical position of first row displayed
    protected startPosition?: number;
    // Total rows in the table
    protected rowCount?: number;
    protected order: RecordOrder;
    // Computed
    // Logical number of data rows displayed; includes count of each data row
    protected dataRowsDisplayed: number;
    // HTML part
    protected top : HTMLDivElement;
    protected scrollBar : ScrollBar;
    protected htmlTable : HTMLTableElement;
    protected tHead : HTMLTableSectionElement;
    protected tBody: HTMLTableSectionElement;
    protected page: FullPage;
    protected currentData: TableDataView;

    public constructor(id: string, page: FullPage) {
        super(id);
        this.top = document.createElement("div");
        this.htmlTable = document.createElement("table");
        this.top.className = "flexcontainer";
        this.scrollBar = new ScrollBar();
        this.top.appendChild(this.htmlTable);
        this.top.appendChild(this.scrollBar.getHTMLRepresentation());
        this.order = new RecordOrder([]);
        this.setPage(page);
    }

    columnIndex(colName: string): number {
        if (this.schema == null)
            return null;
        for (let i = 0; i < this.schema.length; i++)
            if (this.schema[i].name == colName)
                return i;
        return null;
    }

    findColumn(colName: string): IColumnDescription {
        let colIndex = this.columnIndex(colName);
        if (colIndex != null)
            return this.schema[colIndex];
        return null;
    }

    setPage(page: FullPage) {
        if (page == null)
            throw("null FullPage");
        this.page = page;
    }

    getPage() : FullPage {
        if (this.page == null)
            throw("Page not set");
        return this.page;
    }

    getSortOrder(column: string): [boolean, number] {
        for (let i = 0; i < this.order.length(); i++) {
            let o = this.order.get(i);
            if (o.columnDescription.name == column)
                return [o.isAscending, i];
        }
        return null;
    }

    public isVisible(column: string): boolean {
        let so = this.getSortOrder(column);
        return so != null;
     }

    public isAscending(column: string): boolean {
        let so = this.getSortOrder(column);
        if (so == null) return null;
        return so[0];
    }

    public getSortIndex(column: string): number {
        let so = this.getSortOrder(column);
        if (so == null) return null;
        return so[1];
    }

    public getSortArrow(column: string): string {
        let asc = this.isAscending(column);
        if (asc == null)
            return "";
        else if (asc)
            return "&dArr;";
        else
            return "&uArr;";
    }

    private addHeaderCell(thr: Node, cd: ColumnDescription) : HTMLElement {
        let thd = document.createElement("th");
        let label = cd.name;
        if (!this.isVisible(cd.name)) {
            thd.className = "hiddenColumn";
        } else {
            label += " " +
                this.getSortArrow(cd.name) + this.getSortIndex(cd.name);
        }
        thd.innerHTML = label;
        thr.appendChild(thd);
        return thd;
    }

    public showColumn(columnName: string, order: number) : void {
        // order is 0 to hide
        //         -1 to sort descending
        //          1 to sort ascending
        let o = this.order.clone();
        if (order != 0) {
            let col = this.findColumn(columnName);
            if (col == null)
                return;
            o.show({ columnDescription: col, isAscending: order > 0 });
        } else {
            o.hide(columnName);
        }
        this.order = o;  // TODO: this should be set by the renderer
        let rr = this.createRpcRequest("getTableView", o);
        rr.invoke(new TableRenderer(this.getPage(), this, rr));
    }

    public histogram(columnName: string): void {
        let rr = this.createRpcRequest("range", columnName);
        let cd = this.findColumn(columnName);
        rr.invoke(new RangeCollector(cd, this.getPage(), this, rr));
    }

    public refresh(): void {
        this.updateView(this.currentData);
    }

    public updateView(data: TableDataView) : void {
        this.currentData = data;
        this.dataRowsDisplayed = 0;
        this.startPosition = data.startPosition;
        this.rowCount = data.rowCount;
        if (this.schema == null)
            this.schema = data.schema;

        if (this.tHead != null)
            this.tHead.remove();
        if (this.tBody != null)
            this.tBody.remove();
        this.tHead = this.htmlTable.createTHead();
        let thr = this.tHead.appendChild(document.createElement("tr"));

        // These two columns are always shown
        let cds : ColumnDescription[] = [];
        let posCd = new ColumnDescription({
            kind: ContentsKind.Integer,
            name: "(position)",
            allowMissing: false });
        let ctCd = new ColumnDescription({
            kind: ContentsKind.Integer,
            name: "(count)",
            allowMissing: false });

        // Create column headers
        this.addHeaderCell(thr, posCd);
        this.addHeaderCell(thr, ctCd);
        if (this.schema == null)
            return;

        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescription(this.schema[i]);
            cds.push(cd);
            let thd = this.addHeaderCell(thr, cd);
            let menu = new Menu([
                {text: "sort asc", action: () => this.showColumn(cd.name, 1) },
                {text: "sort desc", action: () => this.showColumn(cd.name, -1) }
             ]);
            if (cd.kind != ContentsKind.Json &&
                cd.kind != ContentsKind.String)
                menu.addItem({text: "histogram", action: () => this.histogram(cd.name) });
            if (this.order != null && this.order.find(cd.name) != -1)
                menu.addItem({text: "hide", action: () => this.showColumn(cd.name, 0)});

            thd.onclick = () => menu.toggleVisibility();
            thd.appendChild(menu.getHTMLRepresentation());
        }
        this.tBody = this.htmlTable.createTBody();

        // Add row data
        if (data.rows != null) {
            for (let i = 0; i < data.rows.length; i++)
                this.addRow(data.rows[i], cds);
        }

        // Create table footer
        let footer = this.tBody.insertRow();
        let cell = footer.insertCell(0);
        cell.colSpan = this.schema.length + 2;
        cell.className = "footer";
        cell.textContent = String(this.rowCount + " rows");

        this.updateScrollBar();
    }

    private updateScrollBar(): void {
        if (this.startPosition == null || this.rowCount == null)
            return;
        this.setScroll(this.startPosition / this.rowCount,
            (this.startPosition + this.dataRowsDisplayed) / this.rowCount);
    }

    public getRowCount() : number {
        return this.tBody.childNodes.length;
    }

    public getColumnCount() : number {
        return this.schema.length;
    }

    public getHTMLRepresentation() : HTMLElement {
        return this.top;
    }

    public addRow(row : RowView, cds: ColumnDescription[]) : void {
        let trow = this.tBody.insertRow();

        let cell = trow.insertCell(0);
        cell.className = "rightAlign";
        cell.textContent = String(this.startPosition + this.dataRowsDisplayed);

        cell = trow.insertCell(1);
        cell.className = "rightAlign";
        cell.textContent = String(row.count);

        for (let i = 0; i < cds.length; i++) {
            let cd = cds[i];
            cell = trow.insertCell(i + 2);

            let dataIndex = this.order.find(cd.name);
            if (dataIndex == -1)
                continue;
            if (this.isVisible(cd.name)) {
                cell.className = "rightAlign";
                cell.textContent = String(row.values[dataIndex]);
            }
        }
        this.dataRowsDisplayed += row.count;
    }

    public setScroll(top: number, bottom: number) : void {
        this.scrollBar.setPosition(top, bottom);
    }
}

export class TableRenderer extends Renderer<TableDataView> {
    constructor(page: FullPage,
                protected table: TableView,
                operation: ICancellable) {
        super(page, operation, "Geting table info");
    }

    onNext(value: PartialResult<TableDataView>): void {
        this.progressBar.setPosition(value.done);
        this.table.updateView(value.data);
    }
}

export class RemoteTableReceiver extends RpcReceiver<string> {
    public table: TableView;

    constructor(protected page: FullPage, operation: ICancellable) {
        super(page.progressManager.newProgressBar(operation, "Get schema"),
              page.getErrorReporter());
    }

    private retrieveSchema(): void {
        let rr = this.table.createRpcRequest("getSchema", null);
        rr.invoke(new TableRenderer(this.page, this.table, rr));
    }

    // we expect exactly one reply
    public onNext(value: string): void {
        this.table = new TableView(value, this.page);
        this.page.setHieroDataView(this.table);
    }

    public onCompleted(): void {
        this.finished();
        // Retrieve the table schema
        this.retrieveSchema();
    }
}

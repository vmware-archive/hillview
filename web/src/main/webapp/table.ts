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

import {
    IHtmlElement, Renderer, FullPage, HillviewDataView, formatNumber, significantDigits, percent, KeyCodes,
    ScrollBar, IScrollTarget
} from "./ui";
import {RemoteObject, PartialResult, ICancellable, RpcRequest} from "./rpc";
import Rx = require('rx');
import {RangeCollector, BasicColStats} from "./histogram";
import {Range2DCollector} from "./heatMap";
import {DropDownMenu, ContextMenu, PopupMenu} from "./menu";
import {Converters} from "./util";
import d3 = require('d3');

// The first few classes are direct counterparts to server-side Java classes
// with the same names.  JSON serialization
// of the Java classes produces JSON that can be directly cast
// into these interfaces.

// I can't use an enum for ContentsKind because JSON deserialization does not
// return an enum from a string.

export type ContentsKind = "Category" | "Json" | "String" | "Integer" | "Double" | "Date" | "Interval";

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
export class RecordOrder {
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
    public sortFirst(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.splice(0, 0, cso);
    }
    public show(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index != -1)
            this.sortOrientationList.splice(index, 1);
        this.sortOrientationList.push(cso);
    }
    public showIfNotVisible(cso: ColumnSortOrientation) {
        let index = this.find(cso.columnDescription.name);
        if (index == -1)
            this.sortOrientationList.push(cso);
    }
    public clone(): RecordOrder {
        return new RecordOrder(this.sortOrientationList.slice(0));
    }
    // Returns a new object
    public invert(): RecordOrder {
        let result = new Array<ColumnSortOrientation>(this.sortOrientationList.length);
        for (let i in this.sortOrientationList) {
            let cso = this.sortOrientationList[i];
            result[i] = {
                isAscending: !cso.isAscending,
                columnDescription: cso.columnDescription
            };
        }
        return new RecordOrder(result);
    }

    protected static coToString(cso: ColumnSortOrientation): string {
        return cso.columnDescription.name + " " + (cso.isAscending ? "up" : "down");
    }
    public toString(): string {
        let result = "";
        for (let i = 0; i < this.sortOrientationList.length; i++)
            result += RecordOrder.coToString(this.sortOrientationList[i]);
        return result;
    }
}

export class TableDataView {
    public schema?: Schema;
    // Total number of rows in the complete table
    public rowCount: number;
    public startPosition?: number;
    public rows?: RowView[];
}

export class RangeInfo {
    columnName: string;
    // The following are only used for categorical columns
    firstIndex?: number;
    lastIndex?: number;
    firstValue?: string;
    lastValue?: string;
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
    implements IHtmlElement, HillviewDataView, IScrollTarget {
    protected static initialTableId: string = null;

    // Data view part: received from remote site
    public schema?: Schema;
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
    protected numberedCategories: Set<string>;
    protected selectedColumns: Set<string>;
    protected firstSelectedColumn: string;  // for shift-click

    public constructor(remoteObjectId: string, page: FullPage) {
        super(remoteObjectId);

        this.order = new RecordOrder([]);
        this.numberedCategories = new Set<string>();
        this.setPage(page);
        if (TableView.initialTableId == null)
            TableView.initialTableId = remoteObjectId;
        this.top = document.createElement("div");
        this.top.id = "tableContainer";
        this.top.tabIndex = 1;  // necessary for keyboard events?
        this.top.onkeydown = e => this.keyDown(e);
        this.selectedColumns = new Set<string>();
        this.firstSelectedColumn = null;

        this.top.style.flexDirection = "column";
        this.top.style.display = "flex";
        this.top.style.flexWrap = "nowrap";
        this.top.style.justifyContent = "flex-start";
        this.top.style.alignItems = "stretch";
        let menu = new DropDownMenu([
            { text: "View", subMenu: new ContextMenu([
                { text: "home", action: () => { TableView.goHome(this.page); } },
                { text: "refresh", action: () => { this.refresh(); } },
                { text: "all rows", action: () => { this.showAllRows(); } },
                { text: "no rows", action: () => { this.setOrder(new RecordOrder([])); } }
            ])},
            /*
            { text: "Data", subMenu: new ContextMenu([
                { text: "find", action: () => {} },
                { text: "filter", action: () => {} }
            ]),
            } */
        ]);
        this.top.appendChild(menu.getHTMLRepresentation());
        this.top.appendChild(document.createElement("hr"));
        this.htmlTable = document.createElement("table");
        this.scrollBar = new ScrollBar(this);

        // to force the scroll bar next to the table we put them in yet another div
        let tblAndBar = document.createElement("div");
        tblAndBar.style.flexDirection = "row";
        tblAndBar.style.display = "flex";
        tblAndBar.style.flexWrap = "nowrap";
        tblAndBar.style.justifyContent = "flex-start";
        tblAndBar.style.alignItems = "stretch";
        this.top.appendChild(tblAndBar);
        tblAndBar.appendChild(this.htmlTable);
        tblAndBar.appendChild(this.scrollBar.getHTMLRepresentation());
    }

    // invoked when scrolling has completed
    scrolledTo(position: number): void {
        if (position <= 0) {
            this.begin();
        } else if (position >= 1.0) {
            this.end();
        } else {
            let o = this.order.clone();
            let rr = this.createRpcRequest("quantile", {
                precision: 100,
		        tableSize: this.currentData.rowCount,
                order: o,
                position: position
            });
	    console.log("expecting quantile: " + String(position));
            rr.invoke(new QuantileReceiver(this.getPage(), this, rr, o));
        }
    }

    protected keyDown(ev: KeyboardEvent): void {
        if (ev.keyCode == KeyCodes.pageUp)
            this.pageUp();
        else if (ev.keyCode == KeyCodes.pageDown)
            this.pageDown();
        else if (ev.keyCode == KeyCodes.end)
            this.end();
        else if (ev.keyCode == KeyCodes.home)
            this.begin();
    }

    // TODO: measure window size somehow
    static readonly rowsOnScreen = 20;

    public pageUp(): void {
        if (this.currentData == null || this.currentData.rows.length == 0)
            return;
        if (this.startPosition <= 0) {
            this.page.reportError("Already at the top");
            return;
        }
        let order = this.order.invert();
        let rr = this.createNextKRequest(order, this.currentData.rows[0].values);
        rr.invoke(new TableRenderer(this.getPage(), this, rr, true, order));
    }

    protected begin(): void {
        if (this.currentData == null || this.currentData.rows.length == 0)
            return;
        if (this.startPosition <= 0) {
            this.page.reportError("Already at the top");
            return;
        }
        let o = this.order.clone();
        let rr = this.createNextKRequest(o, null);
        rr.invoke(new TableRenderer(this.getPage(), this, rr, false, o));
    }

    protected end(): void {
        if (this.currentData == null || this.currentData.rows.length == 0)
            return;
        if (this.startPosition + this.dataRowsDisplayed >= this.rowCount - 1) {
            this.page.reportError("Already at the bottom");
            return;
        }
        let order = this.order.invert();
        let rr = this.createNextKRequest(order, null);
        rr.invoke(new TableRenderer(this.getPage(), this, rr, true, order));
    }

    public pageDown(): void {
        if (this.currentData == null || this.currentData.rows.length == 0)
            return;
        if (this.startPosition + this.dataRowsDisplayed >= this.rowCount - 1) {
            this.page.reportError("Already at the bottom");
            return;
        }
        let o = this.order.clone();
        let rr = this.createNextKRequest(o, this.currentData.rows[this.currentData.rows.length - 1].values);
        rr.invoke(new TableRenderer(this.getPage(), this, rr, false, o));
    }

    protected setOrder(o: RecordOrder): void {
        let rr = this.createNextKRequest(o, null);
        rr.invoke(new TableRenderer(this.getPage(), this, rr, false, o));
    }

    protected showAllRows(): void {
        if (this.schema == null) {
            this.page.reportError("No data loaded");
            return;
        }

        let o = this.order.clone();
        for (let i = 0; i < this.schema.length; i++) {
            let c = this.schema[i];
            o.showIfNotVisible({ columnDescription: c, isAscending: true });
        }
        this.setOrder(o);
    }

    // Navigate back to the first table known
    public static goHome(page: FullPage): void {
        if (TableView.initialTableId == null)
            return;

        let table = new TableView(TableView.initialTableId, page);
        page.setHillviewDataView(table);
        let rr = table.createRpcRequest("getSchema", null);
        rr.invoke(new TableRenderer(page, table, rr, false, new RecordOrder([])));
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
            thd.style.fontWeight = "normal";
        } else {
            label += " " +
                this.getSortArrow(cd.name) + this.getSortIndex(cd.name);
        }
        thd.innerHTML = label;
        thr.appendChild(thd);
        return thd;
    }

    // columnName is ignored if the set of selected columns is non-empty
    public showColumn(columnName: string, order: number, first: boolean) : void {
        // order is 0 to hide
        //         -1 to sort descending
        //          1 to sort ascending
        let o = this.order.clone();
        // The set iterator did not seem to work correctly...
        let s: string[] = [];
        if (this.selectedColumns.size != 0) {
            this.selectedColumns.forEach(v => s.push(v));
        } else {
            s.push(columnName);
        }

        for (let i = 0; i < s.length; i++) {
            let colName = s[i];
            let col = this.findColumn(colName);
            if (order != 0 && col != null) {
                if (first)
                    o.sortFirst({columnDescription: col, isAscending: order > 0});
                else
                    o.show({columnDescription: col, isAscending: order > 0});
            } else {
                o.hide(colName);
            }
        }
        this.setOrder(o);
    }

    public histogram(columnName: string): void {
        let cd = this.findColumn(columnName);
        if (cd.kind == "Category" && !this.numberedCategories.has(columnName)) {
            let rr = this.createRpcRequest("uniqueStrings", columnName);
            rr.invoke(new NumberStrings(cd, this.schema, this.getPage(), this, rr));
        } else {
            let rr = this.createRpcRequest("range", { columnName: columnName });
            rr.invoke(new RangeCollector(cd, this.schema, null, this.getPage(), this, rr));
        }
    }

    public createNextKRequest(order: RecordOrder, firstRow: any[]): RpcRequest {
        let nextKArgs = {
            order: order,
            firstRow: firstRow,
            rowsOnScreen: TableView.rowsOnScreen
        };
        return this.createRpcRequest("getNextK", nextKArgs);
    }

    public refresh(): void {
        if (this.currentData == null) {
            this.page.reportError("Nothing to refresh");
            return;
        }
        this.updateView(this.currentData, false, this.order, 0);
    }

    public updateView(data: TableDataView, revert: boolean,
                      order: RecordOrder, elapsedMs: number) : void {
        console.log("updateView " + revert + " " + order);

        this.selectedColumns.clear();
        this.firstSelectedColumn = null;
        this.currentData = data;
        this.dataRowsDisplayed = 0;
        this.startPosition = data.startPosition;
        this.rowCount = data.rowCount;
        this.order = order.clone();
        if (revert) {
            let rowsDisplayed = 0;
            if (data.rows != null) {
                data.rows.reverse();
                rowsDisplayed = data.rows.map(r => r.count)
                    .reduce( (a, b) => { return a + b; }, 0 );
            }
            this.startPosition = this.rowCount - this.startPosition - rowsDisplayed;
            this.order = this.order.invert();
        }
        this.setSchema(data.schema);

        if (this.tHead != null)
            this.tHead.remove();
        if (this.tBody != null)
            this.tBody.remove();
        this.tHead = this.htmlTable.createTHead();
        let thr = this.tHead.appendChild(document.createElement("tr"));

        // These two columns are always shown
        let cds : ColumnDescription[] = [];
        let posCd = new ColumnDescription({
            kind: "Integer",
            name: "(position)",
            allowMissing: false });
        let ctCd = new ColumnDescription({
            kind: "Integer",
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
            thd.className = this.columnClass(cd.name);
            let menu = new PopupMenu([
                {text: "sort asc", action: () => this.showColumn(cd.name, 1, true) },
                {text: "sort desc", action: () => this.showColumn(cd.name, -1, true) },
                {text: "heavy hitters", action: () => this.heavyHitters(cd.name) },
                {text: "heat map", action: () => this.heatMap() }
            ]);
            if (this.order.find(cd.name) >= 0) {
                menu.addItem( {text: "hide", action: () => this.showColumn(cd.name, 0, true) } );
            } else {
                menu.addItem({text: "show", action: () => this.showColumn(cd.name, 1, false) });
            }
            if (cd.kind != "Json" &&
                cd.kind != "String")
                menu.addItem({text: "histogram", action: () => this.histogram(cd.name) });

            thd.onclick = e => this.columnClick(cd.name, e);
            thd.oncontextmenu = e => {
                e.preventDefault();
                this.columnClick(cd.name, e);
                menu.toggleVisibility();
            };
            thd.appendChild(menu.getHTMLRepresentation());
        }
        this.tBody = this.htmlTable.createTBody();

        let tableRowCount = 0;
        // Add row data
        if (data.rows != null) {
            tableRowCount = data.rows.length;
            for (let i = 0; i < data.rows.length; i++)
                this.addRow(data.rows[i], cds);
        }

        // Create table footer
        let footer = this.tBody.insertRow();
        let cell = footer.insertCell(0);
        cell.colSpan = this.schema.length + 2;
        cell.className = "footer";

        let perc = "";
        if (this.rowCount > 0)
            perc = percent(this.dataRowsDisplayed / this.rowCount);
        if (this.startPosition > 0) {
            if (perc != "")
                perc += " ";
            perc += "starting at " + percent(this.startPosition / this.rowCount);
        }
        if (perc != "")
            perc = " (" + perc + ")";

        cell.textContent = "Showing on " + tableRowCount + " rows " +
            formatNumber(this.dataRowsDisplayed) +
            "/" + formatNumber(this.rowCount) + " data rows" + perc;

        this.updateScrollBar();
        this.highlightSelectedColumns();
        this.page.reportError("Operation took " + significantDigits(elapsedMs/1000) + " seconds");
    }

    public setSchema(schema: Schema): void {
        if (this.schema == null)
            this.schema = schema;
    }

    // mouse click on a column
    private columnClick(colName: string, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey) {
            this.firstSelectedColumn = colName;
            if (this.selectedColumns.has(colName))
                this.selectedColumns.delete(colName);
            else
                this.selectedColumns.add(colName);
        } else if (e.shiftKey) {
            if (this.firstSelectedColumn == null)
                this.firstSelectedColumn = colName;
            let first = this.columnIndex(this.firstSelectedColumn);
            let last = this.columnIndex(colName);
            this.selectedColumns.clear();
            if (first > last) { let tmp = first; first = last; last = tmp; }
            for (let i = first; i <= last; i++)
                this.selectedColumns.add(this.schema[i].name);
        } else {
            if ((e.buttons & 2) != 0) {
                // right button
                if (this.selectedColumns.has(colName))
                    // Do nothing if pressed on a selected column
                    return;
            }

            this.firstSelectedColumn = colName;
            this.selectedColumns.clear();
            this.selectedColumns.add(colName);
        }
        this.highlightSelectedColumns();
    }

    private columnClass(colName: string): string {
        let index = this.columnIndex(colName);
        return "col" + String(index);
    }

    private heatMap(): void {
        if (this.selectedColumns.size != 2) {
            this.page.reportError("Must select exactly 2 columns for heat map");
            return;
        }

        let columns: RangeInfo[] = [];
        let cds: ColumnDescription[] = [];
        this.selectedColumns.forEach(v => {
            let colDesc = this.findColumn(v);
            if (colDesc.kind == "String") {
                this.page.reportError("Heat maps not supported for string columns " + colDesc.name);
                return;
            }
            if (colDesc.kind == "Category") {
                this.page.reportError("Heat maps not yet implemented for category columns " + colDesc.name);
                return;
            }
            let ci = new RangeInfo();
            ci.columnName = colDesc.name;
            cds.push(colDesc);
            columns.push(ci);
        });

        let rr = this.createRpcRequest("range2D", columns);
        rr.invoke(new Range2DCollector(cds, this.schema, this.getPage(), this, rr));
    }

    private highlightSelectedColumns(): void {
        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescription(this.schema[i]);
            let name = cd.name;
            let cls = this.columnClass(name);
            let cells = document.getElementsByClassName(cls);
            let selected = this.selectedColumns.has(name);

            for (let i = 0; i < cells.length; i++) {
                let cell = cells[i];
                if (selected)
                    cell.classList.add("selected");
                else
                    cell.classList.remove("selected");
            }
        }
    }

    private updateScrollBar(): void {
        if (this.startPosition == null || this.rowCount == null)
            return;
        if (this.rowCount <= 0 || this.dataRowsDisplayed <= 0)
            // we show everything
            this.setScroll(0, 1);
        else
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

    private heavyHitters(colName: string): void {
        let columns: IColumnDescription[] = [];
        let cso : ColumnSortOrientation[] = [];
        if (this.selectedColumns.size != 0) {
            this.selectedColumns.forEach(v => {
                let colDesc = this.findColumn(v);
                columns.push(colDesc);
                cso.push({ columnDescription: colDesc, isAscending: true });
            });
        } else {
            let colDesc = this.findColumn(colName);
            columns.push(colDesc);
            cso.push({ columnDescription: colDesc, isAscending: true });
        }
        let order = new RecordOrder(cso);
        let rr = this.createRpcRequest("heavyHitters", columns);
        rr.invoke(new HeavyHittersReceiver(this.getPage(), this, rr, columns, order));
    }

    protected static convert(val: any, kind: ContentsKind): string {
        if (kind == "Integer" || kind == "Double")
            return String(val);
        else if (kind == "Date")
            return Converters.dateFromDouble(<number>val).toDateString();
        else if (kind == "Category" || kind == "String" || kind == "Json")
            return <string>val;
        else
            return "?";  // TODO
    }

    public addRow(row : RowView, cds: ColumnDescription[]) : void {
        let trow = this.tBody.insertRow();

        let cell = trow.insertCell(0);
        cell.style.textAlign = "right";
        cell.textContent = significantDigits(this.startPosition + this.dataRowsDisplayed);

        cell = trow.insertCell(1);
        cell.style.textAlign = "right";
        cell.textContent = significantDigits(row.count);

        for (let i = 0; i < cds.length; i++) {
            let cd = cds[i];
            cell = trow.insertCell(i + 2);
            cell.classList.add(this.columnClass(cd.name));
            cell.style.textAlign = "right";

            let dataIndex = this.order.find(cd.name);
            if (dataIndex == -1)
                continue;
            if (this.isVisible(cd.name)) {
                let value = row.values[dataIndex];
                if (value == null) {
                    cell.classList.add("missingData");
                    cell.textContent = "missing";
                } else {
                    cell.textContent = TableView.convert(row.values[dataIndex], cd.kind);
                }
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
                operation: ICancellable,
                protected revert: boolean,
                protected order: RecordOrder) {
        super(page, operation, "Geting table info");
    }

    onNext(value: PartialResult<TableDataView>): void {
        super.onNext(value);
        this.table.updateView(value.data, this.revert, this.order, this.elapsedMilliseconds());
    }
}

export class RemoteTableReceiver extends Renderer<string> {
    public remoteTableId: string;

    constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "Get schema");
    }

    protected getTableSchema(tableId: string) {
        let table = new TableView(tableId, this.page);
        this.page.setHillviewDataView(table);
        let rr = table.createRpcRequest("getSchema", null);
        rr.setStartTime(this.operation.startTime());
        rr.invoke(new TableRenderer(this.page, table, rr, false, new RecordOrder([])));
    }

    public onNext(value: PartialResult<string>): void {
        super.onNext(value);
        if (value.data != null)
            this.remoteTableId = value.data;
    }

    public onCompleted(): void {
        this.finished();
        if (this.remoteTableId == null)
            return;
        this.getTableSchema(this.remoteTableId);
    }
}

class DistinctStrings {
    mySet: string[];
    truncated: boolean;
    rowCount: number;
    missingCount: number;
}

// First step of a histogram for a categorical column:
// create a numbering for the strings
class NumberStrings extends Renderer<DistinctStrings> {
    protected contentsInfo: DistinctStrings;

    public constructor(protected cd: ColumnDescription, protected schema: Schema,
                       page: FullPage, protected obj: RemoteObject, operation: ICancellable) {
        super(page, operation, "Create converter");
        this.contentsInfo = null;
    }

    public onNext(value: PartialResult<DistinctStrings>): void {
        super.onNext(value);
        this.contentsInfo = value.data;
    }

    public onCompleted(): void {
        if (this.contentsInfo == null)
            return;
        super.finished();
        let strings = this.contentsInfo.mySet;
        if (strings.length == 0) {
            this.page.reportError("No data in column");
            return;
        }
        strings.sort();

        let bcs: BasicColStats = {
            momentCount: 0,
            min: 0,
            max: strings.length - 1,
            minObject: strings[0],
            maxObject: strings[strings.length - 1],
            moments: [],
            presentCount: this.contentsInfo.rowCount - this.contentsInfo.missingCount,
            missingCount: this.contentsInfo.missingCount
        };

        let rc = new RangeCollector(this.cd, this.schema, strings, this.page, this.obj, this.operation);
        rc.setValue(bcs);
        rc.onCompleted();
    }
}

class QuantileReceiver extends Renderer<any[]> {
    protected firstRow: any[];

    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Compute quantiles");
    }

    onNext(value: PartialResult<any[]>): any {
        super.onNext(value);
        if (value.data != null)
            this.firstRow = value.data;
    }

    onCompleted(): void {
        super.finished();
        if (this.firstRow == null)
            return;

        let rr = this.tv.createNextKRequest(this.order, this.firstRow);
        rr.setStartTime(this.operation.startTime());
        rr.invoke(new TableRenderer(this.page, this.tv, rr, false, this.order));
    }
}

// The string received is actually the id of a remote object that stores
// the heavy hitters information.
class HeavyHittersReceiver extends Renderer<string> {
    private hitterObjectsId: string;

    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected schema: IColumnDescription[],
                       protected order: RecordOrder) {
        super(page, operation, "Heavy hitters");
        this.hitterObjectsId = null;
    }

    onNext(value: PartialResult<string>): any {
        super.onNext(value);
        if (value.data != null)
            this.hitterObjectsId = value.data;
    }

    onCompleted(): void {
        super.finished();
        if (this.hitterObjectsId == null)
            return;
        let rr = this.tv.createRpcRequest("filterHeavy", {
                hittersId: this.hitterObjectsId,
                schema: this.schema
            });
        rr.setStartTime(this.operation.startTime());
        rr.invoke(new FilterCompleted(this.page, this.tv, rr, this.order));
    }
}

// After filtering receives the id of a remote table.
class FilterCompleted extends Renderer<string> {
    public remoteTableId: string;

    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Filter");
    }

    onNext(value: PartialResult<string>): any {
        super.onNext(value);
        if (value.data != null)
            this.remoteTableId = value.data;
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteTableId == null)
            return;
        let table = new TableView(this.remoteTableId, this.page);
        table.setSchema(this.tv.schema);
        this.page.setHillviewDataView(table);
        let rr = table.createNextKRequest(this.order, null);
        rr.setStartTime(this.operation.startTime());
        rr.invoke(new TableRenderer(this.page, table, rr, false, this.order));
    }
}
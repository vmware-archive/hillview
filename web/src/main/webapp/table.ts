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
    IHtmlElement, FullPage, DataView, formatNumber, significantDigits, percent, KeyCodes,
    ScrollBar, IScrollTarget
} from "./ui";
import {RemoteObject, RpcRequest, Renderer, combineMenu, SelectedObject, CombineOperators, ZipReceiver} from "./rpc";
import Rx = require('rx');
import {BasicColStats} from "./histogramBase";
import {RangeCollector} from "./histogram";
import {PCAProjectionRequest} from "./pca";
import {Range2DCollector} from "./heatMap";
import {TopMenu, TopSubMenu, ContextMenu} from "./menu";
import {Converters, PartialResult, ICancellable} from "./util";
import {EqualityFilterDialog, EqualityFilterDescription} from "./equalityFilter";
import d3 = require('d3');
import {Dialog} from "./dialog";

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
    implements IHtmlElement, DataView, IScrollTarget {
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
    protected contextMenu: ContextMenu; 

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

        let menu = new TopMenu([
            {
                text: "View", subMenu: new TopSubMenu([
                    { text: "Home", action: () => { TableView.goHome(this.page); } },
                    { text: "Refresh", action: () => { this.refresh(); } },
                    { text: "All columns", action: () => { this.showAllRows(); } },
                    { text: "No columns", action: () => { this.setOrder(new RecordOrder([])); } }
                ])
            },
            {
                text: "Combine", subMenu: combineMenu(this)
            },
            {
                text: "Operation", subMenu: new TopSubMenu([
                    {text: "PCA on all numeric columns", action: () => this.pca(true)}
                ])
            }
        ]);
        this.top.appendChild(menu.getHTMLRepresentation());
        this.contextMenu = new ContextMenu();
        this.contextMenu.hide();
        this.top.appendChild(this.contextMenu.getHTMLRepresentation());
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

    reportError(s: string) {
        this.page.reportError(s);
    }

    // combine two views according to some operation
    combine(how: CombineOperators): void {
        let r = SelectedObject.current.getSelected();
        if (r == null) {
            this.reportError("No view selected");
            return;
        }

        let rr = this.createRpcRequest("zip", r.remoteObjectId);
        let o = this.order.clone();
        let finalRenderer = (page: FullPage, operation: ICancellable) =>
            { return new TableOperationCompleted(page, this, operation, o); };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, finalRenderer));
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
            this.reportError("Already at the top");
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
            this.reportError("Already at the top");
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
            this.reportError("Already at the bottom");
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
            this.reportError("Already at the bottom");
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
            this.reportError("No data loaded");
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
        page.setDataView(table);
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
        if (this.selectedColumns.size <= 1) {
            let cd = this.findColumn(columnName);
            if (cd.kind == "Category" && !this.numberedCategories.has(columnName)) {
                let rr = this.createRpcRequest("uniqueStrings", columnName);
                rr.invoke(new NumberStrings(cd, this.schema, this.getPage(), this, rr));
            } else {
                let rr = this.createRpcRequest("range", {columnName: columnName});
                rr.invoke(new RangeCollector(cd, this.schema, null, this.getPage(), this, rr));
            }
        } else if (this.selectedColumns.size == 2) {
            let columns: RangeInfo[] = [];
            let cds: ColumnDescription[] = [];
            this.selectedColumns.forEach(v => {
                let colDesc = this.findColumn(v);
                if (colDesc.kind == "String") {
                    this.reportError("2D Histograms not supported for string columns " + colDesc.name);
                    return;
                }
                if (colDesc.kind == "Category") {
                    this.reportError("2D histograms not yet implemented for category columns " + colDesc.name);
                    return;
                }
                let ci = new RangeInfo();
                ci.columnName = colDesc.name;
                cds.push(colDesc);
                columns.push(ci);
            });

            if (columns.length != 2)
                return;
            let rr = this.createRpcRequest("range2D", columns);
            rr.invoke(new Range2DCollector(cds, this.schema, this.getPage(), this, rr, false));
        } else {
            this.reportError("Must select 1 or 2 columns for histogram");
            return;
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
            this.reportError("Nothing to refresh");
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
            thd.onclick = e => this.columnClick(cd.name, e);
            thd.oncontextmenu = e => {
                e.preventDefault();
                this.columnClick(cd.name, e);
                if (e.ctrlKey && (e.buttons & 1) != 0) {
                    // Ctrl + click is interpreted as a right-click on macOS.
                    // This makes sure it's interpreted as a column click with Ctrl.
                    return;
                }

                this.contextMenu.clear();
                this.contextMenu.addItem({text: "Sort ascending", action: () => this.showColumn(cd.name, 1, true) });
                this.contextMenu.addItem({text: "Sort descending", action: () => this.showColumn(cd.name, -1, true) });
                this.contextMenu.addItem({text: "Heavy hitters...", action: () => this.heavyHitters(cd.name) });
                this.contextMenu.addItem({text: "Heat map", action: () => this.heatMap() });
                this.contextMenu.addItem({text: "Select numeric columns", action: () => this.selectNumericColumns()});
                this.contextMenu.addItem({text: "PCA", action: () => this.pca() });

                if (this.order.find(cd.name) >= 0) {
                    this.contextMenu.addItem({text: "Hide", action: () => this.showColumn(cd.name, 0, true)});
                } else {
                    this.contextMenu.addItem({text: "Show", action: () => this.showColumn(cd.name, 1, false)});
                }
                if (cd.kind != "Json" && cd.kind != "String")
                    this.contextMenu.addItem({text: "Histogram", action: () => this.histogram(cd.name) });
                if (cd.kind == "Json" || cd.kind == "String" || cd.kind == "Category" || cd.kind == "Integer")
                    this.contextMenu.addItem({text: "Filter...", action: () => this.equalityFilter(cd.name)});
                
                // Spawn the menu at the mouse's location
                this.contextMenu.move(e.pageX - 1, e.pageY - 1);
                this.contextMenu.show();
            };
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
        this.reportError("Operation took " + significantDigits(elapsedMs/1000) + " seconds");
    }

    public setSchema(schema: Schema): void {
        if (this.schema == null)
            this.schema = schema;
    }

    // mouse click on a column
    private columnClick(colName: string, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey) {
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

    private selectNumericColumns(): void {
        this.selectedColumns.clear();
        for (let i = 0; i < this.schema.length; i++) {
            let kind = this.schema[i].kind;
            if (kind == "Integer" || kind == "Double")
                this.selectedColumns.add(this.schema[i].name);
        }
        this.highlightSelectedColumns();
    }

    private columnClass(colName: string): string {
        let index = this.columnIndex(colName);
        return "col" + String(index);
    }

    private runFilter(filter: EqualityFilterDescription): void {
        let rr = this.createRpcRequest("filterEquality", filter);
        rr.invoke(new TableOperationCompleted(this.page, this, rr, this.order));
    }

    private equalityFilter(colname: string, value?: string, complement?: boolean): void {
        if (value == null) {
            let ef = new EqualityFilterDialog(this.findColumn(colname));
            ef.setAction(() => this.runFilter(ef.getFilter()));
            ef.show();
        } else {
            let efd: EqualityFilterDescription = {
                columnDescription: this.findColumn(colname),
                compareValue: value,
                complement: (complement == null ? false : complement)
            }
            this.runFilter(efd);
        }
    }

    private pca(): void {
        let colNames: string[] = [];
        this.selectedColumns.forEach(col => colNames.push(col));

        let valid = true;
        let message = "";
        colNames.forEach((colName) => {
            let kind = this.findColumn(colName).kind;
            if (kind != "Double" && kind != "Integer") {
                valid = false;
                message += "\n  * Column '" + colName  + "' is not numeric.";
            }
        });

        if (colNames.length < 3) {
            this.reportError("Not enough numeric columns. Need at least 3. There are " + colNames.length);
            return;
        }

        if (valid) {
            let correlationMatrixRequest = {
                columnNames: colNames
            };
            let rr = this.createRpcRequest("correlationMatrix", correlationMatrixRequest);
            rr.invoke(new CorrelationMatrixReceiver(this.getPage(), this, rr, this.order));
        } else {
            this.reportError("Only numeric columns are supported for PCA:" + message);
        }
    }

    private heatMap(): void {
        if (this.selectedColumns.size != 2) {
            this.reportError("Must select exactly 2 columns for heat map");
            return;
        }

        let columns: RangeInfo[] = [];
        let cds: ColumnDescription[] = [];
        this.selectedColumns.forEach(v => {
            let colDesc = this.findColumn(v);
            if (colDesc.kind == "String") {
                this.reportError("Heat maps not supported for string columns " + colDesc.name);
                return;
            }
            if (colDesc.kind == "Category") {
                this.reportError("Heat maps not yet implemented for category columns " + colDesc.name);
                return;
            }
            let ci = new RangeInfo();
            ci.columnName = colDesc.name;
            cds.push(colDesc);
            columns.push(ci);
        });

        if (columns.length != 2)
            // some error has occurred
            return;
        let rr = this.createRpcRequest("range2D", columns);
        rr.invoke(new Range2DCollector(cds, this.schema, this.getPage(), this, rr, true));
    }

    private highlightSelectedColumns(): void {
        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescription(this.schema[i]);
            let name = cd.name;
            let cls = this.columnClass(name);
            let cells = this.tHead.getElementsByClassName(cls);
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

    private runHeavyHitters(colName: string, percent: number) {
        if (percent < .01 || percent > 100) {
            this.reportError("Percentage must be between .01 and 100");
            return;
        }
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
        let rr = this.createRpcRequest("heavyHitters", { columns: columns, amount: percent });
        rr.invoke(new HeavyHittersReceiver(this.getPage(), this, rr, columns, order));
    }

    private heavyHitters(colName: string): void {
        let d = new Dialog("Heavy hitters");
        d.addTextField("percent", "Threshold (%)", "Double");
        d.setAction(() => this.runHeavyHitters(colName, d.getFieldValueAsNumber("percent")));
        d.show();
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

    private drawDataRange(cell: HTMLElement, position: number, count: number) : void {
        let w = Math.max(0.01, count / this.rowCount);
        let x = position / this.rowCount;
        if (x + w > 1)
            x = 1 - w;
        cell.classList.add('dataRange');
        d3.select(cell).append('svg')
            .append("g").append("rect")
            .attr("x", x)
            .attr("y", 0)
            .attr("width", w) // 0.01 corresponds to 1 pixel
            .attr("height", 1);
    }

    public addRow(row : RowView, cds: ColumnDescription[]) : void {
        let trow = this.tBody.insertRow();

        let position = this.startPosition + this.dataRowsDisplayed;

        let cell = trow.insertCell(0);
        this.drawDataRange(cell, position, row.count);

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
                    let cellValue : string = TableView.convert(row.values[dataIndex], cd.kind);
                    cell.textContent = cellValue;
                    if (cd.kind == "String" || cd.kind == "Json" || cd.kind == "Category" || cd.kind == "Integer") {
                        cell.oncontextmenu = e => {
                            e.preventDefault();
                            this.contextMenu.clear();
                            this.contextMenu.addItem({text: "Filter for " + cellValue, action: () => this.equalityFilter(cd.name, cellValue)});
                            this.contextMenu.addItem({text: "Filter for not " + cellValue, action: () => this.equalityFilter(cd.name, cellValue, true)});
                            this.contextMenu.move(e.pageX - 1, e.pageY - 1);
                            this.contextMenu.show();
                        };
                    }
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
        this.page.setDataView(table);
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
        rr.invoke(new TableOperationCompleted(this.page, this.tv, rr, this.order));
    }
}

// The string received is actually the id of a remote object that stores
// the correlation matrix information
class CorrelationMatrixReceiver extends Renderer<string> {
    private correlationMatrixObjectsId: string;

    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Correlation matrix");
        this.correlationMatrixObjectsId = null;
    }

    onNext(value: PartialResult<string>): any {
        super.onNext(value);
        if (value.data != null)
            this.correlationMatrixObjectsId = value.data;
    }

    onCompleted(): void {
        super.finished();
        if (this.correlationMatrixObjectsId == null)
            return;
        let rr = this.tv.createRpcRequest("projectToEigenVectors", {
                id: this.correlationMatrixObjectsId
        });
        rr.setStartTime(this.operation.startTime());
        rr.invoke(new RemoteTableReceiver(this.page, rr));
    }
}

// After operating on a table receives the id of a new remote table.
class TableOperationCompleted extends Renderer<string> {
    public remoteTableId: string;

    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Table operation");
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
        this.page.setDataView(table);
        let rr = table.createNextKRequest(this.order, null);
        rr.setStartTime(this.operation.startTime());
        rr.invoke(new TableRenderer(this.page, table, rr, false, this.order));
    }
}

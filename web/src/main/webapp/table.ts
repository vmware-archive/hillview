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

import {
    FullPage, KeyCodes, ScrollBar, IScrollTarget, SpecialChars
} from "./ui";
import {Renderer, combineMenu, SelectedObject, CombineOperators, OnCompleteRenderer} from "./rpc";
import {RangeCollector} from "./histogram";
import {Range2DCollector} from "./heatMap";
import {TopMenu, SubMenu, ContextMenu} from "./menu";
import { Converters, PartialResult, ICancellable, percent, formatNumber, significantDigits, formatDate} from "./util";
import {EqualityFilterDialog, EqualityFilterDescription} from "./equalityFilter";
import {Dialog} from "./dialog";
import {
    Schema, RowView, RecordOrder, IColumnDescription, ColumnDescription, ColumnSortOrientation,
    ContentsKind, RangeInfo, RemoteTableObjectView, ZipReceiver, RemoteTableRenderer, RemoteTableObject,
    DistinctStrings
} from "./tableData";
import {CategoryCache} from "./categoryCache";
import {HeatMapArrayDialog} from "./heatMapArray";
import {ColumnConverter, HLogLog} from "./columnConverter";
import {DataRange} from "./vis"
import {HeavyHittersView} from "./heavyhittersview";
import {SchemaView} from "./schemaview";
import {LAMPDialog} from "./lamp";
import {StateMachine} from "./stateMachine";

/**
 * The serialization of a NextKList Java object
 */
// This is the serialization of a NextKList Java object
export class NextKList {
    public schema?: Schema;
    // Total number of rows in the complete table
    public rowCount: number;
    public startPosition?: number;
    public rows?: RowView[];
}

/**
 * Displays a table in the browser.
 */
export class TableView extends RemoteTableObjectView implements IScrollTarget {
    // Data view part: received from remote site
    public schema?: Schema;
    // Logical position of first row displayed
    protected startPosition?: number;
    // Total rows in the table
    protected rowCount?: number;
    protected order: RecordOrder;
    // Logical number of data rows displayed; includes count of each data row
    protected dataRowsDisplayed: number;
    protected scrollBar : ScrollBar;
    protected htmlTable : HTMLTableElement;
    protected tHead : HTMLTableSectionElement;
    protected tBody: HTMLTableSectionElement;
    protected currentData: NextKList;
    protected selectedColumns: StateMachine;
    protected contextMenu: ContextMenu;
    protected cellsPerColumn: Map<string, HTMLElement[]>;
    static firstTable: RemoteTableObject;

    public constructor(remoteObjectId: string, page: FullPage) {
        super(remoteObjectId, page);

        this.order = new RecordOrder([]);
        if (TableView.firstTable == null)
            TableView.firstTable = this;
        this.topLevel = document.createElement("div");
        this.topLevel.id = "tableContainer";
        this.topLevel.tabIndex = 1;  // necessary for keyboard events?
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.selectedColumns = new StateMachine();

        this.topLevel.style.flexDirection = "column";
        this.topLevel.style.display = "flex";
        this.topLevel.style.flexWrap = "nowrap";
        this.topLevel.style.justifyContent = "flex-start";
        this.topLevel.style.alignItems = "stretch";

        let menu = new TopMenu([
            {
                text: "View", subMenu: new SubMenu([
                    { text: "Full dataset", action: () => { TableView.fullDataset(this.page); } },
                    { text: "Refresh", action: () => { this.refresh(); } },
                    { text: "All columns", action: () => { this.showAllRows(); } },
                    { text: "No columns", action: () => { this.setOrder(new RecordOrder([])); } },
                    { text: "Schema", action: () => { this.viewSchema();}}
                ])
            },
            {
                text: "Combine", subMenu: combineMenu(this, page.pageId)
            }
        ]);
        //this.topLevel.appendChild(menu.getHTMLRepresentation());
        this.page.setMenu(menu);
        this.contextMenu = new ContextMenu();
        this.topLevel.appendChild(this.contextMenu.getHTMLRepresentation());
        this.topLevel.appendChild(document.createElement("hr"));
        this.htmlTable = document.createElement("table");
        this.scrollBar = new ScrollBar(this);

        // to force the scroll bar next to the table we put them in yet another div
        let tblAndBar = document.createElement("div");
        tblAndBar.style.flexDirection = "row";
        tblAndBar.style.display = "flex";
        tblAndBar.style.flexWrap = "nowrap";
        tblAndBar.style.justifyContent = "flex-start";
        tblAndBar.style.alignItems = "stretch";
        this.topLevel.appendChild(tblAndBar);
        tblAndBar.appendChild(this.htmlTable);
        tblAndBar.appendChild(this.scrollBar.getHTMLRepresentation());
    }

    reportError(s: string) {
        this.page.reportError(s);
    }

    /**
     * Combine two views according to some operation: intersection, union, etc.
     */
    combine(how: CombineOperators): void {
        let r = SelectedObject.current.getSelected();
        if (r == null) {
            this.reportError("No view selected");
            return;
        }

        let rr = this.createZipRequest(r);
        let o = this.order.clone();
        let finalRenderer = (page: FullPage, operation: ICancellable) =>
            { return new TableOperationCompleted(page, this, operation, o); };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, finalRenderer));
    }

    /**
     * Invoked when scrolling has completed.
     */
    scrolledTo(position: number): void {
        if (this.currentData == null)
            return;

        if (position <= 0) {
            this.begin();
        } else if (position >= 1.0) {
            this.end();
        } else {
            let o = this.order.clone();
            let rr = this.createQuantileRequest(this.currentData.rowCount, o, position);
                console.log("expecting quantile: " + String(position));
            rr.invoke(new QuantileReceiver(this.getPage(), this, rr, o));
        }
    }

    /**
     * Event handler called when a key is pressed
     */
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

    /**
     * Scroll one page up
     */
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
    public static fullDataset(page: FullPage): void {
        if (TableView.firstTable == null)
            return;

        let table = new TableView(TableView.firstTable.remoteObjectId, page);
        page.setDataView(table);
        let rr = table.createGetSchemaRequest();
        rr.invoke(new TableRenderer(page, table, rr, false, new RecordOrder([])));
    }

    public static allColumnNames(schema: Schema): string[] {
        if (schema == null)
            return null;
        let colNames = [];
        for (let i = 0; i < schema.length; i++)
            colNames.push(schema[i].name)
        return colNames;
    }

    public static uniqueColumnName(schema: Schema, prefix: string): string {
        let existingNames = TableView.allColumnNames(schema);
        let name = prefix;
        let i = 0;
        while (existingNames.indexOf(name) >= 0) {
            name = prefix + `_${i}`;
            i++;
        }
        return name;
    }

    public static columnIndex(schema: Schema, colName: string): number {
        if (schema == null)
            return null;
        for (let i = 0; i < schema.length; i++)
            if (schema[i].name == colName)
                return i;
        return null;
    }

    public static findColumn(schema: Schema, colName: string): IColumnDescription {
        let colIndex = TableView.columnIndex(schema, colName);
        if (colIndex != null)
            return schema[colIndex];
        return null;
    }

    public static dropColumns(schema: Schema, filter: (IColumnDescription) => boolean): Schema {
        let cols: IColumnDescription[] = [];
        for (let i=0; i < schema.length; i++) {
            let c = schema[i];
            if (!filter(c))
                cols.push(c);
        }
        return cols;
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

    public showColumns(order: number, first: boolean) : void {
        // order is 0 to hide
        //         -1 to sort descending
        //          1 to sort ascending
        let o = this.order.clone();
        // The set iterator did not seem to work correctly...
        this.getSelectedColNames().forEach(colName => {
            let col = TableView.findColumn(this.schema, colName);
            if (order != 0 && col != null) {
                if (first)
                    o.sortFirst({columnDescription: col, isAscending: order > 0});
                else
                    o.show({columnDescription: col, isAscending: order > 0});
            } else
                o.hide(colName);
        });
        this.setOrder(o);
    }

    public histogram(heatMap: boolean): void {
        if (this.selectedColumns.size() > 2) {
            this.reportError("Must select 1 or 2 columns for histogram");
            return;
        }

        let cds: ColumnDescription[] = [];
        let catColumns: string[] = [];  // categorical columns
        this.getSelectedColNames().forEach(v => {
            let colDesc = TableView.findColumn(this.schema, v);
            if (colDesc.kind == "String") {
                this.reportError("Histograms not supported for string columns " + colDesc.name);
                return;
            }
            if (colDesc.kind == "Category")
                catColumns.push(v);
            cds.push(colDesc);
        });

        if (cds.length != this.selectedColumns.size())
            // some error occurred
            return;

        let twoDimensional = (cds.length == 2);
        // Continuation invoked after the distinct strings have been obtained
        let cont = (operation: ICancellable) => {
            let rangeInfo: RangeInfo[] = [];
            let distinct: DistinctStrings[] = [];

            cds.forEach(v => {
                let colName = v.name;
                let ri: RangeInfo;
                if (v.kind == "Category") {
                    let ds = CategoryCache.instance.getDistinctStrings(colName);
                    if (ds == null)
                    // Probably an error has occurred
                        return;
                    distinct.push(ds);
                    ri = ds.getRangeInfo(colName);
                } else {
                    distinct.push(null);
                    ri = new RangeInfo(colName);
                }
                rangeInfo.push(ri);
            });

            if (rangeInfo.length != cds.length)
                // some error occurred in loop
                return;

            if (twoDimensional) {
                let rr = this.createRange2DRequest(rangeInfo[0], rangeInfo[1]);
                rr.chain(operation);
                rr.invoke(new Range2DCollector(cds, this.schema, distinct, this.getPage(), this, false, rr, heatMap));
            } else {
                let rr = this.createRangeRequest(rangeInfo[0]);
                rr.chain(operation);
                let title = "Histogram " + cds[0].name;
                rr.invoke(new RangeCollector(title, cds[0], this.schema, distinct[0],
                    this.getPage(), this, false, rr));
            }
        };

        // Get the categorical data and invoke the continuation
        CategoryCache.instance.retrieveCategoryValues(this, catColumns, this.getPage(), cont);
    }

    public refresh(): void {
        if (this.currentData == null) {
            this.reportError("Nothing to refresh");
            return;
        }
        this.updateView(this.currentData, false, this.order, 0);
    }

    public updateView(data: NextKList, revert: boolean,
                      order: RecordOrder, elapsedMs: number) : void {
        this.selectedColumns.clear();
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
            thd.onclick = e => this.columnClick(i, e);
            thd.oncontextmenu = e => {
                e.preventDefault();
                this.columnClick(i, e);
                if (e.ctrlKey && (e.button == 1)) {
                    // Ctrl + click is interpreted as a right-click on macOS.
                    // This makes sure it's interpreted as a column click with Ctrl.
                    return;
                }

                this.contextMenu.clear();
                if (this.order.find(cd.name) >= 0) {
                    this.contextMenu.addItem({text: "Hide", action: () => this.showColumns(0, true)});
                } else {
                    this.contextMenu.addItem({text: "Show", action: () => this.showColumns(1, false)});
                }
                //this.contextMenu.addItem({text: "Drop", action: () => this.dropColumns() });
                this.contextMenu.addItem({text: "Estimate Distinct Elements", action: () => this.hLogLog()});
                this.contextMenu.addItem({text: "Sort ascending", action: () => this.showColumns(1, true) });
                this.contextMenu.addItem({text: "Sort descending", action: () => this.showColumns(-1, true) });
                this.contextMenu.addItem({text: "Histogram", action: () => this.histogram(false) });
                this.contextMenu.addItem({text: "Heat map", action: () => this.heatMap() });
                this.contextMenu.addItem({text: "Heavy hitters...", action: () => this.heavyHitters(false) });
                //this.contextMenu.addItem({text: "Heavy hitters (Streaming)...", action: () => this.heavyHitters(true) });
                //this.contextMenu.addItem({text: "Select numeric columns", action: () => this.selectNumericColumns()});
                this.contextMenu.addItem({text: "PCA...", action: () => this.pca() });
                this.contextMenu.addItem({text: "LAMP...", action: () => this.lamp() });
                this.contextMenu.addItem({text: "Filter...", action: () => this.equalityFilter(cd.name)});
                this.contextMenu.addItem({text: "Convert...", action: () => ColumnConverter.dialog(cd.name, TableView.allColumnNames(this.schema), this)});

                // Spawn the menu at the mouse's location
                this.contextMenu.move(e.pageX - 1, e.pageY - 1);
                this.contextMenu.show();
            };
        }
        this.tBody = this.htmlTable.createTBody();

        this.cellsPerColumn = new Map<string, HTMLElement[]>();
        cds.forEach((cd) => this.cellsPerColumn.set(cd.name, []));
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
        this.page.reportTime(elapsedMs);
    }

    dropColumns(): void {
        this.currentData.schema = TableView.dropColumns(this.schema,
                c => (this.getSelectedColNames().indexOf(c.name) != -1));
        this.refresh();
    }

    public setSchema(schema: Schema): void {
        if (schema != null)
            this.schema = schema;
    }

    // mouse click on a column
    private columnClick(colNum: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey)
            this.selectedColumns.changeState( "Ctrl", colNum);
        else if (e.shiftKey)
            this.selectedColumns.changeState( "Shift", colNum);
        else {
            if (e.button == 2) {
                // right button
                if (this.selectedColumns.has(colNum))
                // Do nothing if pressed on a selected column
                    return;
            }
            this.selectedColumns.changeState("NoKey", colNum);
        }
        this.highlightSelectedColumns();
    }

    // noinspection JSUnusedLocalSymbols
    private selectNumericColumns(): void {
        this.selectedColumns.clear();
        let count = 0;
        for (let i = 0; i < this.schema.length; i++) {
            let kind = this.schema[i].kind;
            if (kind == "Integer" || kind == "Double") {
                this.selectedColumns.add(i);
                count++;
            }
        }
        this.reportError(`Selected ${count} numeric columns.`);
        this.highlightSelectedColumns();
    }

    private columnClass(colName: string): string {
        let index = TableView.columnIndex(this.schema, colName);
        return "col" + String(index);
    }

    private runFilter(filter: EqualityFilterDescription, kind: ContentsKind): void {
        let rr = this.createFilterEqualityRequest(filter);
        let newPage = new FullPage("Filtered: " + filter.column + " is " +
            (filter.complement ? "not " : "") +
            TableView.convert(filter.compareValue, kind), this.page);
        this.page.insertAfterMe(newPage);
        rr.invoke(new RemoteTableReceiver(newPage, rr));
    }

    private equalityFilter(colName: string, value?: string, complement?: boolean): void {
        let cd = TableView.findColumn(this.schema, colName);
        if (value == null) {
            let ef = new EqualityFilterDialog(cd);
            ef.setAction(() => this.runFilter(ef.getFilter(), cd.kind));
            ef.show();
        } else {
            if (cd.kind == "Date") {
                // Parse the date in Javascript; the Java Date parser is very bad
                let date = new Date(value);
                value = Converters.doubleFromDate(date).toString();
            }
            let efd: EqualityFilterDescription = {
                column: cd.name,
                compareValue: value,
                complement: (complement == null ? false : complement)
            };
            this.runFilter(efd, cd.kind);
        }
    }

    private hLogLog(): void {
        if (this.selectedColumns.size() != 1) {
            this.reportError("Only one column must be selected");
            return;
        }
        let colName = this.getSelectedColNames()[0];
        let rr = this.createHLogLogRequest(colName);
        let rec = new CountReceiver(this.getPage(), rr, colName);
        rr.invoke(rec);
    }

    public getSelectedColNames(): string[] {
        let colNames: string[] = [];
        this.selectedColumns.getStates().forEach(i => colNames.push(this.schema[i].name));
        return colNames;
    }

    private checkNumericColumns(colNames: string[], atLeast: number = 3): [boolean, string] {
        if (colNames.length < atLeast) {
            let message = `\nNot enough columns. Need at least ${atLeast}. There are ${colNames.length}`;
            return [false, message];
        }
        let valid = true;
        let message = "";
        colNames.forEach((colName) => {
            let kind = TableView.findColumn(this.schema, colName).kind;
            if (kind != "Double" && kind != "Integer") {
                valid = false;
                message += "\n  * Column '" + colName  + "' is not numeric.";
            }
        });

        return [valid, message];
    }

    private pca(): void {
        let colNames = this.getSelectedColNames();
        let [valid, message] = this.checkNumericColumns(colNames);
        if (valid) {
            let pcaDialog = new Dialog("Principal Component Analysis");
            pcaDialog.addTextField("numComponents", "Number of components", "Integer", "2");
            pcaDialog.setAction(() => {
                let numComponents: number = pcaDialog.getFieldValueAsInt("numComponents");
                if (numComponents < 1 || numComponents > colNames.length) {
                    this.reportError("Number of components for PCA must be between 1 (incl.) " +
                        "and the number of selected columns, " + colNames.length + " (incl.). (" +
                        numComponents + " does not satisfy this.)");
                    return;
                }
                let rr = this.createCorrelationMatrixRequest(colNames);
                rr.invoke(new CorrelationMatrixReceiver(this.getPage(), this, rr, this.order, numComponents));
            });
            pcaDialog.show();
        } else {
            this.reportError("Not valid for PCA:" + message);
        }
    }

    private lamp(): void {
        let colNames = this.getSelectedColNames();
        let [valid, message] = this.checkNumericColumns(colNames);
        if (valid) {
            let dialog = new LAMPDialog(colNames, this.getPage(), this.schema, this);
            dialog.show();
        } else {
            this.reportError("Not valid for LAMP:" + message);
        }
    }


    private heatMapArray(): void {
        let colNames: string[] = this.getSelectedColNames();
        let dialog = new HeatMapArrayDialog(colNames, this.getPage(), this.schema, this);
        dialog.show();
    }

    private heatMap(): void {
        if (this.selectedColumns.size() == 3) {
            this.heatMapArray();
            return;
        }
        if (this.selectedColumns.size() != 2) {
            this.reportError("Must select exactly 2 columns for heat map");
            return;
        }

        this.histogram(true);
    }

    private highlightSelectedColumns(): void {
        for (let i = 0; i < this.schema.length; i++) {
            let name = this.schema[i].name;
            let cls = this.columnClass(name);
            let headers = this.tHead.getElementsByClassName(cls);
            let cells = this.cellsPerColumn.get(name);
            let selected = this.selectedColumns.has(i);
            for (let i = 0; i < headers.length; i++) {
                let header = headers[i];
                if (selected)
                    header.classList.add("selected");
                else
                    header.classList.remove("selected");
            }
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

    public getTotalRowCount() : number {
        return this.rowCount;
    }

    public getRowCount() : number {
        return this.tBody.childNodes.length;
    }

    public getColumnCount() : number {
        return this.schema.length;
    }

    public viewSchema(): void {
        let newPage = new FullPage("Schema", this.page);
        let sv = new SchemaView(this.remoteObjectId, newPage, this.schema, this.rowCount);
        newPage.setDataView(sv);
        this.page.insertAfterMe(newPage);
    }

    private runHeavyHitters(percent: number, isMG: boolean) {
        if (percent == null || percent < .1 || percent > 100) {
            this.reportError("Percentage must be between .1 and 100");
            return;
        }
        let columns: IColumnDescription[] = [];
        let cso : ColumnSortOrientation[] = [];
        this.getSelectedColNames().forEach(v => {
            let colDesc = TableView.findColumn(this.schema, v);
            columns.push(colDesc);
            cso.push({ columnDescription: colDesc, isAscending: true });
        });
        let order = new RecordOrder(cso);
        let rr = this.createHeavyHittersRequest(columns, percent, this.getTotalRowCount(), isMG);
        rr.invoke(new HeavyHittersReceiver(this.getPage(), this, rr, columns, order));
    }

    private heavyHitters(isMG: boolean): void {
        let title = "Heavy hitters on ";
        let cols: string[] = this.getSelectedColNames();
        if (cols.length <= 1) {
            title += " " + cols[0];
        } else {
            title += cols.length + " columns";
        }
        let d = new Dialog(title);
        d.addTextField("percent", "Threshold (%)", "Double", "1");
        d.setAction(() => {
            let amount = d.getFieldValueAsNumber("percent");
            if (amount != null)
                this.runHeavyHitters(amount, isMG)
        });
        d.show();
    }

    public static convert(val: any, kind: ContentsKind): string {
        if (kind == "Integer" || kind == "Double")
            return String(val);
        else if (kind == "Date")
            return formatDate(Converters.dateFromDouble(<number>val));
        else if (kind == "Category" || kind == "String" || kind == "Json")
            return <string>val;
        else
            return val.toString();  // TODO
    }

    public addRow(row : RowView, cds: ColumnDescription[]) : void {
        let trow = this.tBody.insertRow();

        let position = this.startPosition + this.dataRowsDisplayed;

        let cell = trow.insertCell(0);
        let dataRange = new DataRange(position, row.count, this.rowCount);
        cell.appendChild(dataRange.getDOMRepresentation());

        cell = trow.insertCell(1);
        cell.style.textAlign = "right";
        cell.textContent = significantDigits(row.count);

        for (let i = 0; i < cds.length; i++) {
            let cd = cds[i];
            cell = trow.insertCell(i + 2);
            cell.classList.add(this.columnClass(cd.name));
            cell.style.textAlign = "right";

            this.cellsPerColumn.get(cd.name).push(cell);

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
                    cell.oncontextmenu = e => {
                        e.preventDefault();
                        this.contextMenu.clear();
                        this.contextMenu.addItem({text: "Filter for " + cellValue,
                            action: () => this.equalityFilter(cd.name, cellValue)});
                        this.contextMenu.addItem({text: "Filter for not " + cellValue,
                            action: () => this.equalityFilter(cd.name, cellValue, true)});
                        this.contextMenu.move(e.pageX - 1, e.pageY - 1);
                        this.contextMenu.show();
                    };
                }
            }

        }
        this.dataRowsDisplayed += row.count;
    }

    public setScroll(top: number, bottom: number) : void {
        this.scrollBar.setPosition(top, bottom);
    }
}

class CountReceiver extends OnCompleteRenderer<HLogLog> {
    constructor(page: FullPage, operation: ICancellable,
                protected colName: string) {
        super(page, operation, "HyperLogLog");
    }

    run(data: HLogLog): void {
        let timeInMs = this.elapsedMilliseconds();
        this.page.reportError("Distinct values in column \'" +
            this.colName + "\' " + SpecialChars.approx + String(data.distinctItemCount) + "\n" +
            "Operation took " + significantDigits(timeInMs/1000) + " seconds");
    }
}

export class TableRenderer extends Renderer<NextKList> {
    constructor(page: FullPage,
                protected table: TableView,
                operation: ICancellable,
                protected reverse: boolean,
                protected order: RecordOrder) {
        super(page, operation, "Getting table info");
    }

    onNext(value: PartialResult<NextKList>): void {
        super.onNext(value);
        this.table.updateView(value.data, this.reverse, this.order, this.elapsedMilliseconds());
        this.table.scrollIntoView();
    }
}

export class RemoteTableReceiver extends RemoteTableRenderer {
    constructor(page: FullPage, operation: ICancellable) {
        super(page, operation, "Get schema");
    }

    protected getTableSchema() {
        let table = new TableView(this.remoteObject.remoteObjectId, this.page);
        this.page.setDataView(table);
        let rr = table.createGetSchemaRequest();
        rr.chain(this.operation);
        rr.invoke(new TableRenderer(this.page, table, rr, false, new RecordOrder([])));
    }

    public onCompleted(): void {
        this.finished();
        if (this.remoteObject == null)
            return;
        this.getTableSchema();
    }
}

class QuantileReceiver extends OnCompleteRenderer<any[]> {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Compute quantiles");
    }

    run(firstRow: any[]): void {
        let rr = this.tv.createNextKRequest(this.order, firstRow);
        rr.chain(this.operation);
        rr.invoke(new TableRenderer(this.page, this.tv, rr, false, this.order));
    }
}

export interface TopList {
    top: NextKList;
    heavyHittersId: string;
}

/**
 * This method handles the outcome of the Misra-Gries sketch for finding Heavy Hitters.
 */
class HeavyHittersReceiver extends OnCompleteRenderer<TopList> {
     public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected schema: IColumnDescription[],
                       protected order: RecordOrder) {
        super(page, operation, "Heavy hitters");
    }

    run(data: TopList): void {
        let newPage = new FullPage("Heavy hitters", this.page);
        let hhv = new HeavyHittersView(data, newPage, this.tv, this.schema, this.order, true);
        newPage.setDataView(hhv);
        this.page.insertAfterMe(newPage);
        hhv.fill(data.top, this.elapsedMilliseconds());
    }
}

class CorrelationMatrixReceiver extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder,
                       private numComponents: number) {
        super(page, operation, "Correlation matrix");
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteObject == null)
            return;
        let rr = this.tv.createProjectToEigenVectorsRequest(
                this.remoteObject, this.numComponents);
        rr.chain(this.operation);
        rr.invoke(new RemoteTableReceiver(this.page, rr));
    }
}

// After operating on a table receives the id of a new remote table.
export class TableOperationCompleted extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Table operation");
    }

    onCompleted(): void {
        super.finished();
        if (this.remoteObject == null)
            return;
        let table = new TableView(this.remoteObject.remoteObjectId, this.page);
        table.setSchema(this.tv.schema);
        this.page.setDataView(table);
        let rr = table.createNextKRequest(this.order, null);
        rr.chain(this.operation);
        rr.invoke(new TableRenderer(this.page, table, rr, false, this.order));
    }
}

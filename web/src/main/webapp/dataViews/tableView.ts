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

import {Renderer, OnCompleteRenderer} from "../rpc";
import {TopMenu, SubMenu, ContextMenu} from "../ui/menu";
import {
    Converters, PartialResult, ICancellable, percent, formatNumber, significantDigits,
    formatDate
} from "../util";
import {EqualityFilterDialog, EqualityFilterDescription} from "./equalityFilter";
import {Dialog, FieldKind} from "../ui/dialog";
import {CategoryCache} from "../categoryCache";
import {ColumnConverter, ConverterDialog} from "./columnConverter";
import {DataRange} from "../ui/dataRange"
import {IScrollTarget, ScrollBar} from "../ui/scroll";
import {FullPage} from "../ui/fullPage";
import {missingHtml, SpecialChars} from "../ui/ui";
import {SelectionStateMachine} from "../ui/selectionStateMachine";

import {RangeCollector} from "./histogramView";
import {Range2DCollector} from "./heatMapView";
import {HeatMapArrayDialog} from "./trellisHeatMapView";
import {HeavyHittersView} from "./heavyHittersView";
import {SchemaView} from "./schemaView";
import {LAMPDialog} from "./lampView";
import {
    IColumnDescription, RecordOrder, RowView, Schema, ColumnDescription, RangeInfo,
    ContentsKind, asContentsKind, ColumnSortOrientation, NextKList, TopList, CombineOperators, TableSummary, HLogLog,
    RemoteObjectId, allContentsKind, CreateColumnInfo
} from "../javaBridge";
import {RemoteTableObject, RemoteTableObjectView, RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {DistinctStrings} from "../distinctStrings";
import {combineMenu, SelectedObject} from "../selectedObject";
import {IDataView} from "../ui/dataview";

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
    protected selectedColumns: SelectionStateMachine;
    protected contextMenu: ContextMenu;
    protected cellsPerColumn: Map<string, HTMLElement[]>;

    public constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId, page: FullPage) {
        super(remoteObjectId, originalTableId, page);

        this.order = new RecordOrder([]);
        this.topLevel = document.createElement("div");
        this.topLevel.id = "tableContainer";
        this.topLevel.tabIndex = 1;  // necessary for keyboard events?
        this.topLevel.onkeydown = e => this.keyDown(e);
        this.selectedColumns = new SelectionStateMachine();

        this.topLevel.style.flexDirection = "column";
        this.topLevel.style.display = "flex";
        this.topLevel.style.flexWrap = "nowrap";
        this.topLevel.style.justifyContent = "flex-start";
        this.topLevel.style.alignItems = "stretch";

        let menu = new TopMenu([
            {
                text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                    { text: "Full dataset",
                        action: () => { this.fullDataset(); },
                        help: "Show the initial dataset, prior to any filtering operations."
                    },
                    { text: "Refresh",
                        action: () => { this.refresh(); },
                        help: "Redraw this view."
                    },
                    { text: "All columns",
                        action: () => { this.showAllRows(); },
                        help: "Make all columns visible."
                    },
                    { text: "No columns",
                        action: () => { this.setOrder(new RecordOrder([])); },
                        help: "Make all columns invisible"
                    },
                    { text: "Schema",
                        action: () => { this.viewSchema();},
                        help: "Browse the list of columns of this table and choose a subset to visualize."
                    }
                ])
            },
            {
                text: "Combine", help: "Combine data in two separate views.", subMenu: combineMenu(this, page.pageId)
            }
        ]);

        this.page.setMenu(menu);
        this.contextMenu = new ContextMenu(this.topLevel);
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
        let r = SelectedObject.instance.getSelected(this, this.getPage().getErrorReporter());
        if (r == null)
            return;

        let rr = this.createZipRequest(r);
        let o = this.order.clone();
        let finalRenderer = (page: FullPage, operation: ICancellable) =>
            { return new TableOperationCompleted(page, this, operation, o); };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.originalTableId, finalRenderer));
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
        if (ev.code == "PageUp")
            this.pageUp();
        else if (ev.code == "PageDown")
            this.pageDown();
        else if (ev.code == "End")
            this.end();
        else if (ev.code == "Home")
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, true, order));
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o));
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, true, order));
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o));
    }

    protected setOrder(o: RecordOrder): void {
        let rr = this.createNextKRequest(o, null);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o));
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
    public fullDataset(): void {
        let table = new TableView(this.originalTableId, this.originalTableId, this.page);
        this.page.setDataView(table);
        let rr = table.createGetSchemaRequest();
        rr.invoke(new NextKReceiver(this.page, table, rr, false, new RecordOrder([])));
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

    private addHeaderCell(thr: Node, cd: ColumnDescription, help: string) : HTMLElement {
        let thd = document.createElement("th");
        thd.classList.add("noselect");
        let label = cd.name;
        if (!this.isVisible(cd.name)) {
            thd.style.fontWeight = "normal";
        } else {
            label += " " +
                this.getSortArrow(cd.name) + this.getSortIndex(cd.name);
        }
        thd.title = help;
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
        if (this.selectedColumns.size() < 1 || this.selectedColumns.size() > 2) {
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
                    let ds = CategoryCache.instance.getDistinctStrings(this.originalTableId, colName);
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
        let thd = this.addHeaderCell(thr, posCd, "Position within sorted order.");
        thd.oncontextmenu = () => {};
        thd = this.addHeaderCell(thr, ctCd, "Number of occurrences.");
        thd.oncontextmenu = () => {};
        if (this.schema == null)
            return;

        for (let i = 0; i < this.schema.length; i++) {
            let cd = new ColumnDescription(this.schema[i]);
            cds.push(cd);
            let title = "Column type is " + cd.kind + (cd.allowMissing ? ", can have missing data" : "") +
                ".\nA mouse click with the right button will open a menu.";
            let thd = this.addHeaderCell(thr, cd, title);
            thd.className = this.columnClass(cd.name);
            thd.onclick = e => this.columnClick(i, e);
            thd.oncontextmenu = e => {
                this.columnClick(i, e);
                if (e.ctrlKey && (e.button == 1)) {
                    // Ctrl + click is interpreted as a right-click on macOS.
                    // This makes sure it's interpreted as a column click with Ctrl.
                    return;
                }

                let selectedCount = this.selectedColumns.size();
                this.contextMenu.clear();
                if (this.order.find(cd.name) >= 0) {
                    this.contextMenu.addItem({
                        text: "Hide",
                        action: () => this.showColumns(0, true),
                        help: "Hide the data in the selected columns"
                    }, true);
                } else {
                    this.contextMenu.addItem({
                        text: "Show",
                        action: () => this.showColumns(1, false),
                        help: "Show the data in the selected columns."
                    }, true);
                }

                //this.contextMenu.addItem({text: "Drop", action: () => this.dropColumns() });
                this.contextMenu.addItem({
                    text: "Estimate Distinct Elements",
                    action: () => this.hLogLog(),
                    help: "Compute an estimate of the number of different values that appear in the selected column."
                }, selectedCount == 1);
                this.contextMenu.addItem({
                    text: "Sort ascending",
                    action: () => this.showColumns(1, true),
                    help: "Sort the data first on this colum, in increasing order."
                }, true);
                this.contextMenu.addItem({
                    text: "Sort descending",
                    action: () => this.showColumns(-1, true),
                    help: "Sort the data first on this column, in decreasing order"
                }, true);
                this.contextMenu.addItem({
                    text: "Histogram",
                    action: () => this.histogram(false),
                    help: "Plot the data in the selected columns as a histogram.  Applies to one or two columns only. " +
                    "The data cannot be of type String."
                }, selectedCount >= 1 && selectedCount <= 2);
                this.contextMenu.addItem({
                    text: "Heatmap",
                    action: () => this.heatMap(),
                    help: "Plot the data in the selected columns as a heatmap or as a Trellis plot of heatmaps. " +
                    "Applies to two or three columns only."
                }, selectedCount >= 2 && selectedCount <= 3);
                this.contextMenu.addItem({
                    text: "Heavy hitters...",
                    action: () => this.heavyHitters(false),
                    help: "Find the values that occur most frequently in the selected columns."
                }, true);
                this.contextMenu.addItem({
                    text: "PCA...",
                    action: () => this.pca(true),
                    help: "Perform Principal Component Analysis on a set of numeric columns. " +
                    "This produces a smaller set of columns that preserve interesting properties of the data."
                }, selectedCount > 1 &&
                    this.getSelectedColNames().reduce( (a, b) => a && this.isNumericColumn(b), true) );
                this.contextMenu.addItem({
                    text: "LAMP...",
                    action: () => this.lamp(),
                    help: "Perform a Local Affine Multidimensional Projection of the data in a set of numeric columns." +
                    "This produces a 2D view of the data which can be manually adjusted.  Note: this operation is rather slow."
                }, selectedCount > 1 &&
                    this.getSelectedColNames().reduce( (a, b) => a && this.isNumericColumn(b), true) );
                this.contextMenu.addItem({
                    text: "Filter...",
                    action: () => this.equalityFilter(cd.name, null, true),
                    help: "Eliminate data that matches/does not match a specific value in a selected column."
                }, selectedCount == 1);
                this.contextMenu.addItem({
                    text: "Convert...",
                    action: () => this.convert(cd.name),
                    help: "Convert the data in the selected column to a different data type."
                }, selectedCount == 1);
                this.contextMenu.addItem({
                    text: "Create column...",
                    action: () => this.addColumn(),
                    help: "Add a new column computed from the selected columns."
                }, true);
                this.contextMenu.show(e);
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

    addColumn(): void {
        let dialog = new Dialog(
            "Add column", "Specify a JavaScript function which computes the values in a new column.");
        dialog.addTextField(
            "outColName", "Column name", FieldKind.String, null, "Name to use for the generated column.");
        dialog.addSelectField(
            "outColKind", "Data type", allContentsKind, "Category", "Type of data in the generated column.");
        dialog.addMultiLineTextField("function", "Function",
            "function map(row) {", "  return row['col'];", "}",
            "A JavaScript function that computes the values for each row of the generated column." +
            "The function has a single argument 'row'.  The row is a JavaScript map that can be indexed with " +
            "a column name (a string) and which produces a value.");
        dialog.setCacheTitle("CreateDialog");
        dialog.setAction(() => this.createColumn(dialog));
        dialog.show();
    }

    createColumn(dialog: Dialog): void {
        let col = dialog.getFieldValue("outColName");
        let kind = dialog.getFieldValue("outColKind");
        let fun = "function map(row) {" + dialog.getFieldValue("function") + "}";
        let selColumns = this.getSelectedColNames();
        let subSchema = TableView.dropColumns(this.schema, c => (selColumns.indexOf(c) < 0));
        let arg: CreateColumnInfo = {
            jsFunction: fun,
            outputColumn: col,
            outputKind: asContentsKind(kind),
            schema: subSchema
        };
        let rr = this.createCreateColumnRequest(arg);
        let rec = new RemoteTableReceiver(
            this.getPage(), rr, "New column " + col, true, this.originalTableId);
        rr.invoke(rec);
    }

    /**
     * Convert the data in a column to a different column kind.
     */
    convert(colName: string): void {
        let cd = new ConverterDialog(colName, TableView.allColumnNames(this.schema));
        cd.setAction(
            () => {
                let kindStr = cd.getFieldValue("newKind");
                let kind: ContentsKind = asContentsKind(kindStr);
                let converter: ColumnConverter = new ColumnConverter(
                    cd.getFieldValue("columnName"), kind, cd.getFieldValue("newColumnName"), this);
                converter.run();
            });
        cd.show();
    }

    /*
    dropColumns(): void {
        this.currentData.schema = TableView.dropColumns(this.schema,
                c => (this.getSelectedColNames().indexOf(c.name) != -1));
        this.refresh();
    }*/

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
        let title = "Filtered: " + filter.column + " is " +
            (filter.complement ? "not " : "") +
            TableView.convert(filter.compareValue, kind);
        rr.invoke(new RemoteTableReceiver(this.page, rr, title, true, this.originalTableId));
    }

    private equalityFilter(colName: string, value: string, showMenu: boolean, complement?: boolean): void {
        let cd = TableView.findColumn(this.schema, colName);
        if (showMenu) {
            let ef = new EqualityFilterDialog(cd);
            ef.setAction(() => this.runFilter(ef.getFilter(), cd.kind));
            ef.show();
        } else {
            if (value != null && cd.kind == "Date") {
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

    private isNumericColumn(colName: string): boolean {
        let kind = TableView.findColumn(this.schema, colName).kind;
        return kind == "Double" || kind == "Integer";
    }

    private checkNumericColumns(colNames: string[], atLeast: number = 3): [boolean, string] {
        if (colNames.length < atLeast) {
            let message = `\nNot enough columns. Need at least ${atLeast}. There are ${colNames.length}`;
            return [false, message];
        }
        let valid = true;
        let message = "";
        colNames.forEach((colName) => {
            if (!this.isNumericColumn(colName)) {
                valid = false;
                message += "\n  * Column '" + colName  + "' is not numeric.";
            }
        });

        return [valid, message];
    }

    private pca(toSample: boolean): void {
        let colNames = this.getSelectedColNames();
        let [valid, message] = this.checkNumericColumns(colNames);
        if (valid) {
            let pcaDialog = new Dialog("Principal Component Analysis",
                "Projects a set of numeric columns to a smaller set of numeric columns while preserving the 'shape' "+
                " of the data as much as possible.");
            pcaDialog.addTextField("numComponents", "Number of components", FieldKind.Integer, "2",
                "Number of dimensions to project to.  Must be an integer bigger than 1 and " +
                "smaller than the number of selected columns");
            pcaDialog.addTextField("projectionName", "Name for Projected columns", FieldKind.String,
                "PCA",
                "The projected columns will appear with this name followed by a number starting from 0");
            pcaDialog.setCacheTitle("PCADialog");
            pcaDialog.setAction(() => {
                let numComponents: number = pcaDialog.getFieldValueAsInt("numComponents");
                let projectionName: string = pcaDialog.getFieldValue("projectionName");
                if (numComponents < 1 || numComponents > colNames.length) {
                    this.reportError("Number of components for PCA must be between 1 (incl.) " +
                        "and the number of selected columns, " + colNames.length + " (incl.). (" +
                        numComponents + " does not satisfy this.)");
                    return;
                }
                let rr = this.createCorrelationMatrixRequest(colNames, this.getTotalRowCount(), toSample);
                rr.invoke(new CorrelationMatrixReceiver(this.getPage(), this, rr, this.order, numComponents, projectionName));
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
        let dialog = new HeatMapArrayDialog(colNames, this.getPage(), this.schema, this, false);
        dialog.show();
    }

    private heatMap(): void {
        if (this.selectedColumns.size() == 3) {
            this.heatMapArray();
            return;
        }
        if (this.selectedColumns.size() != 2) {
            this.reportError("Must select exactly 2 columns for heatmap");
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
        let newPage = new FullPage("Schema of ", "Schema", this.page);
        let sv = new SchemaView(this.remoteObjectId, this.originalTableId, newPage, this.schema, this.rowCount);
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
        let d = new Dialog(title, "Find the most frequent values in the selected columns.");
        d.addTextField("percent", "Threshold (%)", FieldKind.Double, "1",
            "All values that appear in a dataset with a frequency above this value (as a percent) " +
            "will be included in the heavy hitters set.  Must be a number between 0.1 and 100.");
        d.setAction(() => {
            let amount = d.getFieldValueAsNumber("percent");
            if (amount != null)
                this.runHeavyHitters(amount, isMG)
        });
        d.setCacheTitle("HeavyHittersDialog");
        d.show();
    }

    /**
     * Convert a value in the table to a html string representation.
     * @param val                  Value to convert.
     * @param {ContentsKind} kind  Type of value.
     */
    public static convert(val: any, kind: ContentsKind): string {
        if (val == null)
            return missingHtml;
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
        cell.classList.add("noselect");
        cell.textContent = significantDigits(row.count);
        cell.title = "Number of rows that have these values: " + formatNumber(row.count);

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

                let cellValue : string;
                if (value == null) {
                    cell.classList.add("missingData");
                    cellValue = "missing";
                } else {
                    cellValue = TableView.convert(row.values[dataIndex], cd.kind);
                    value = cellValue;
                }
                cell.textContent = cellValue;
                cell.classList.add("noselect");
                cell.title = "Right click will popup a menu.";
                cell.oncontextmenu = e => {
                    this.contextMenu.clear();
                    this.contextMenu.addItem({text: "Filter for " + cellValue,
                        action: () => this.equalityFilter(cd.name, value, false),
                        help: "Keep only the rows that have this value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Filter for not " + cellValue,
                        action: () => this.equalityFilter(cd.name, value, false, true),
                        help: "Keep only the rows that have a different value in this column."
                    }, true);
                    this.contextMenu.show(e);
                };
            } else {
                // disable context menu
                cell.oncontextmenu = () => false;
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

/**
 * Receives the NextK rows from a table and displays them.
 */
export class NextKReceiver extends Renderer<NextKList> {
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
    }
}

/**
 * Receives the ID for a remote table and initiates a request to get the
 * table schema.
 */
export class RemoteTableReceiver extends RemoteTableRenderer {
    /**
     * Create a renderer for a new table.
     * @param {FullPage} page   Parent page initiating this request.
     * @param {ICancellable} operation   Operation that will bring the results.
     * @param {string} title             Title to use for resulting page; if null the parent page is used.
     * @param forceTableView             If true the resulting view is always a table.
     * @param originalTableId            Id of original table from which everything derives.
     */
    constructor(page: FullPage, operation: ICancellable, protected title: string,
                protected forceTableView: boolean, originalTableId: RemoteObjectId) {
        super(page, operation, "Get schema", originalTableId);
    }

    public run(): void {
        super.run();
        let rr = this.remoteObject.createGetSchemaRequest();
        rr.chain(this.operation);
        rr.invoke(new SchemaReceiver(this.page, rr, this.remoteObject,
            this.title, this.forceTableView));
    }
}

/**
 * Receives a Schema and displays the resulting table.
 */
class SchemaReceiver extends OnCompleteRenderer<TableSummary> {
    /**
     * Create a schema receiver for a new table.
     * @param {FullPage} page            Parent page initiating this request.
     * @param {ICancellable} operation   Operation that will bring the results.
     * @param remoteObject               Table object.
     * @param {string} title             Title to use for resulting page; if null the parent page is used.
     * @param forceTableView             If true the resulting view is always a table.
     */
    constructor(page: FullPage, operation: ICancellable, protected remoteObject: RemoteTableObject,
                protected title: string, protected forceTableView) {
        super(page, operation, "")
    }

    run(summary: TableSummary): void {
        let page: FullPage;
        let dataView: IDataView;
        if (summary.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }

        if (summary.schema.length > 20 && this.title != null && !this.forceTableView) {
            page = new FullPage("Schema of " + this.title, "Schema", this.page);
            dataView = new SchemaView(this.remoteObject.remoteObjectId,
                this.remoteObject.originalTableId, page, summary.schema, summary.rowCount);
        } else {
            if (this.title != null)
                page = new FullPage(this.title, "Table", this.page);
            else
                page = this.page;
            let nk: NextKList = {
                schema: this.value.schema,
                rowCount: this.value.rowCount,
                startPosition: 0,
                rows: []
            };

            let order = new RecordOrder([]);
            let table = new TableView(
                this.remoteObject.remoteObjectId, this.remoteObject.originalTableId, page);
            table.updateView(nk, false, order, this.elapsedMilliseconds());
            dataView = table;
        }
        this.page.insertAfterMe(page);
        page.setDataView(dataView);
    }
}

/**
 * Receives a row which is the result of an approximate quantile request and
 * initiates a request to get the NextK rows after this one.
 */
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
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order));
    }
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
        let newPage = new FullPage("Heavy hitters", "HeavyHitters", this.page);
        let hhv = new HeavyHittersView(data, newPage, this.tv, this.schema, this.order, true);
        newPage.setDataView(hhv);
        this.page.insertAfterMe(newPage);
        hhv.fill(data.top, this.elapsedMilliseconds());
    }
}

/**
 * Receives the result of a PCA computation and initiates the request
 * to project the specified columns using the projection matrix.
 */
class CorrelationMatrixReceiver extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder,
                       private numComponents: number,
                       private projectionName: string) {
        super(page, operation, "Correlation matrix", tv.originalTableId);
    }

    run(): void {
        super.run();
        let rr = this.tv.createProjectToEigenVectorsRequest(
                this.remoteObject, this.numComponents, this.projectionName);
        rr.chain(this.operation);
        rr.invoke(new RemoteTableReceiver(
            this.page, rr, "Data with PCA projection columns", true, this.tv.originalTableId));
    }
}

/**
 * Receives the id of a remote table and
 * initiates a request to display the nextK rows from this table.
 */
export class TableOperationCompleted extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Table operation", tv.originalTableId);
    }

    run(): void {
        super.run();
        let table = new TableView(
            this.remoteObject.remoteObjectId, this.remoteObject.originalTableId, this.page);
        table.setSchema(this.tv.schema);
        this.page.setDataView(table);
        let rr = table.createNextKRequest(this.order, null);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, table, rr, false, this.order));
    }
}

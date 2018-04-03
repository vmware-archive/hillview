///<reference path="../tableTarget.ts"/>
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
    formatDate, Comparison, cloneToSet
} from "../util";
import {Dialog, FieldKind} from "../ui/dialog";
import {ColumnConverter, ConverterDialog} from "./columnConverter";
import {DataRange} from "../ui/dataRange"
import {IScrollTarget, ScrollBar} from "../ui/scroll";
import {FullPage} from "../ui/fullPage";
import {missingHtml} from "../ui/ui";
import {SelectionStateMachine} from "../ui/selectionStateMachine";

import {SchemaView} from "./schemaView";
import {
    IColumnDescription, RecordOrder, RowSnapshot, Schema, ContentsKind, asContentsKind, NextKList,
    CombineOperators, TableSummary, RemoteObjectId, FindResult, ComparisonFilterDescription, ColumnSortOrientation
} from "../javaBridge";
import {RemoteTableObject, RemoteTableRenderer, ZipReceiver} from "../tableTarget";
import {IDataView} from "../ui/dataview";
import {TableViewBase} from "./tableViewBase";
import {Dataset} from "../dataset";
import {SchemaClass} from "../schemaClass";
//import {LAMPDialog} from "./lampView";

/**
 * Displays a table in the browser.
 */
export class TableView extends TableViewBase implements IScrollTarget {
    // Data view part: received from remote site
    // Logical position of first row displayed
    protected startPosition?: number;
    protected order: RecordOrder;
    // Logical number of data rows displayed; includes count of each data row
    protected dataRowsDisplayed: number;
    protected scrollBar: ScrollBar;
    protected htmlTable: HTMLTableElement;
    protected tHead: HTMLTableSectionElement;
    protected tBody: HTMLTableSectionElement;
    protected currentData: NextKList;
    protected contextMenu: ContextMenu;
    protected cellsPerColumn: Map<string, HTMLElement[]>;
    protected selectedColumns = new SelectionStateMachine();
    protected messageBox: HTMLElement;

    public constructor(remoteObjectId: RemoteObjectId, dataset: Dataset, page: FullPage) {
        super(remoteObjectId, dataset, page);

        this.selectedColumns = new SelectionStateMachine();
        this.order = new RecordOrder([]);
        this.topLevel = document.createElement("div");
        this.topLevel.id = "tableContainer";
        this.topLevel.tabIndex = 1;  // necessary for keyboard events?
        this.topLevel.onkeydown = e => this.keyDown(e);

        this.topLevel.style.flexDirection = "column";
        this.topLevel.style.display = "flex";
        this.topLevel.style.flexWrap = "nowrap";
        this.topLevel.style.justifyContent = "flex-start";
        this.topLevel.style.alignItems = "stretch";

        let menu = new TopMenu([
            this.saveAsMenu(),
            {
                text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                    /*
                    { text: "Full dataset",
                        action: () => this.fullDataset(),
                        help: "Show the initial dataset, prior to any filtering operations."
                    },*/
                    { text: "Refresh",
                        action: () => this.refresh(),
                        help: "Redraw this view."
                    },
                    /*
                    { text: "All columns",
                        action: () => this.showAllColumns(),
                        help: "Make all columns visible."
                    },
                    */
                    { text: "No columns",
                        action: () => this.setOrder(new RecordOrder([])),
                        help: "Make all columns invisible"
                    },
                    { text: "Schema",
                        action: () => this.viewSchema(),
                        help: "Browse the list of columns of this table and choose a subset to visualize."
                    }
                ])
            },
            this.chartMenu(),
            {
                text: "Filter", help: "Search specific values",
                subMenu: new SubMenu([{
                    text: "Find...",
                    help: "Search for a string in the visible columns",
                    action: () => this.find() }, {
                    text: "Filter...",
                    help: "Filter rows that contain a specific value",
                    action: () => this.showFilterDialog(null, this.order) }, {
                    text: "Compare...",
                    help: "Filter rows by comparing with a specific value",
                    action: () => this.showCompareDialog(null, this.order) },
                ])
            },
            dataset.combineMenu(this, page.pageId)
        ]);

        this.page.setMenu(menu);
        this.contextMenu = new ContextMenu(this.topLevel);
        this.topLevel.appendChild(document.createElement("hr"));
        this.htmlTable = document.createElement("table");
        this.htmlTable.className = "tabularDisplay";
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

        this.messageBox = document.createElement("div");
        this.topLevel.appendChild(this.messageBox);
    }

    find(): void {
        if (this.order.length() == 0) {
            this.reportError("Find operates in the displayed column, but no column is currently visible.");
            return;
        }
        let dialog = new Dialog("Find", "Find a string/pattern");
        dialog.addTextField("string", "String to search", FieldKind.String, null, "Pattern to look for");
        dialog.addBooleanField("substring", "Match substrings", false,
            "If checked a substring will match.");
        dialog.addBooleanField("regex", "Treat as regular expression", false,
            "If true the string is treated as a regular expression");
        dialog.addBooleanField("caseSensitive", "case sensitive", true,
            "if checked search will match uppercase/lowercase exactly.");
        dialog.setCacheTitle("FindMenu");

        dialog.setAction(() => this.search(dialog.getFieldValue("string"),
            dialog.getBooleanValue("regex"),
            dialog.getBooleanValue("substring"),
            dialog.getBooleanValue("caseSensitive")));
        dialog.show();
    }

    search(toFind: string, regex: boolean, substring: boolean, caseSensitive: boolean): void {
        if (toFind == "") {
            this.reportError("Search string cannot be empty");
            return;
        }
        if (this.currentData.rows.length == 0) {
            this.reportError("No data to search in");
            return;
        }
        let o = this.order.clone();
        let rr = this.createFindRequest(o, this.currentData.rows[0].values, toFind, regex, substring, caseSensitive);
        rr.invoke(new FindReceiver(this.getPage(), this, rr, o));
    }

    /**
     * Combine two views according to some operation: intersection, union, etc.
     */
    combine(how: CombineOperators): void {
        let r = this.dataset.getSelected();
        if (r.first == null)
            return;

        let rr = this.createZipRequest(r.first);
        let o = this.order.clone();
        let finalRenderer = (page: FullPage, operation: ICancellable) => {
            return new TableOperationCompleted(page, this.schema, operation, o, this.dataset);
        };
        rr.invoke(new ZipReceiver(this.getPage(), rr, how, this.dataset, finalRenderer));
    }

    getSelectedColCount(): number {
        return this.selectedColumns.size();
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, true, order, null));
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, true, order, null));
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
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    protected setOrder(o: RecordOrder): void {
        let rr = this.createNextKRequest(o, null);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    protected showAllColumns(): void {
        if (this.schema == null) {
            this.reportError("No data loaded");
            return;
        }

        let o = this.order.clone();
        for (let i = 0; i < this.schema.length; i++) {
            let c = this.schema.get(i);
            o.addColumnIfNotVisible({ columnDescription: c, isAscending: true });
        }
        this.setOrder(o);
    }

    /*
     Navigate back to the first table known
    public fullDataset(): void {
        let table = new TableView(this.originalTableId, this.originalTableId, this.page);
        this.page.setDataView(table);
        let rr = table.createGetSchemaRequest();
        rr.invoke(new NextKReceiver(this.page, table, rr, false, new RecordOrder([])));
    }
    */

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

    private addHeaderCell(thr: Node, cd: IColumnDescription, help: string): HTMLElement {
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

    public showColumns(order: number, first: boolean): void {
        // order is 0 to hide
        //         -1 to sort descending
        //          1 to sort ascending
        let o = this.order.clone();
        // The set iterator did not seem to work correctly...
        this.getSelectedColNames().forEach(colName => {
            let col = this.schema.find(colName);
            if (order != 0 && col != null) {
                if (first)
                    o.sortFirst({columnDescription: col, isAscending: order > 0});
                else
                    o.addColumn({columnDescription: col, isAscending: order > 0});
            } else
                o.hide(colName);
        });
        this.setOrder(o);
    }

    public refresh(): void {
        if (this.currentData == null) {
            this.reportError("Nothing to refresh");
            return;
        }
        this.updateView(this.currentData, false, this.order, null, 0);
    }

    public updateView(data: NextKList, revert: boolean,
                      order: RecordOrder, foundCount: number,
                      elapsedMs: number): void {
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
                rowsDisplayed = data.rows.map(r => r.count).reduce( (a, b) => {return a + b;}, 0 );
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
        let cds: IColumnDescription[] = [];
        let posCd: IColumnDescription = {
            kind: "Integer",
            name: "(position)"
        };
        let ctCd: IColumnDescription = {
            kind: "Integer",
            name: "(count)"
        };

        // Create column headers
        let thd = this.addHeaderCell(thr, posCd, "Position within sorted order.");
        thd.oncontextmenu = () => {};
        thd = this.addHeaderCell(thr, ctCd, "Number of occurrences.");
        thd.oncontextmenu = () => {};
        if (this.schema == null)
            return;

        for (let i = 0; i < this.schema.length; i++) {
            let cd = this.schema.get(i);
            cds.push(cd);
            let title = "Column type is " + cd.kind +
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

                this.contextMenu.addItem({
                    text: "Drop",
                    action: () => this.dropColumns(),
                    help: "Eliminate the selected columns from the view."
                }, selectedCount != 0);
                this.contextMenu.addItem({
                    text: "Estimate distinct elements",
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
                    text: "Frequent Elements...",
                    action: () => this.heavyHittersDialog(),  // switch between Sampling/MG based on HeavyHittersView.switchToMG
                    help: "Find the values that occur most frequently in the selected columns."
                }, true);
                this.contextMenu.addItem({
                    text: "PCA...",
                    action: () => this.pca(true),
                    help: "Perform Principal Component Analysis on a set of numeric columns. " +
                    "This produces a smaller set of columns that preserve interesting properties of the data."
                }, selectedCount > 1 &&
                    this.getSelectedColNames().reduce( (a, b) => a && this.isNumericColumn(b), true) );
                /*
                this.contextMenu.addItem({
                    text: "LAMP...",
                    action: () => this.lamp(),
                    help: "Perform a Local Affine Multidimensional Projection of the data in a set of numeric columns." +
                    "This produces a 2D view of the data which can be manually adjusted.  Note: this operation is rather slow."
                }, selectedCount > 1 &&
                    this.getSelectedColNames().reduce( (a, b) => a && this.isNumericColumn(b), true) );
                    */
                this.contextMenu.addItem({
                    text: "Filter...",
                    action: () => {
                        let colName = this.getSelectedColNames()[0];
                        let cd = this.schema.find(colName);
                        this.showFilterDialog(cd.name, this.order);
                    },
                    help: "Eliminate data that matches/does not match a specific value."
                }, selectedCount == 1);
                this.contextMenu.addItem({
                    text: "Compare...",
                    action: () => {
                        let colName = this.getSelectedColNames()[0];
                        this.showCompareDialog(colName, this.order);
                    },
                    help : "Eliminate data that matches/does not match a specific value."
                }, selectedCount == 1);
                this.contextMenu.addItem({
                    text: "Convert...",
                    action: () => this.convert(cd.name),
                    help: "Convert the data in the selected column to a different data type."
                }, selectedCount == 1);
                this.contextMenu.addItem({
                    text: "Create column...",
                    action: () => this.addColumn(this.order),
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

        let message = "Showing on " + tableRowCount + " rows " +
            formatNumber(this.dataRowsDisplayed) +
            "/" + formatNumber(this.rowCount) + " data rows" + perc;
        if (foundCount != null)
            message = foundCount.toString() + " matching rows<br>" + message;
        this.messageBox.innerHTML = message;

        this.updateScrollBar();
        this.highlightSelectedColumns();
        this.page.reportTime(elapsedMs);
    }

    filterOnValue(colName: string, value: string, comparison: Comparison): void {
        let cd = this.schema.find(colName);
        if (value != null && cd.kind == "Date") {
            // Parse the date in Javascript; the Java Date parser is very bad
            let date = new Date(value);
            value = Converters.doubleFromDate(date).toString();
        }
        let cfd: ComparisonFilterDescription = {
            column: colName,
            compareValue: value,
            comparison: comparison,
        };
        this.runComparisonFilter(cfd, this.order);
    }

    /**
     * Convert the data in a column to a different column kind.
     */
    convert(colName: string): void {
        let cd = new ConverterDialog(colName, this.schema.columnNames);
        cd.setAction(
            () => {
                let kindStr = cd.getFieldValue("newKind");
                let kind: ContentsKind = asContentsKind(kindStr);
                let converter: ColumnConverter = new ColumnConverter(
                    cd.getFieldValue("columnName"), kind, cd.getFieldValue("newColumnName"), this,
                    this.order, this.page);
                converter.run();
            });
        cd.show();
    }

    dropColumns(): void {
        let selected = cloneToSet(this.getSelectedColNames());
        let schema = this.schema.filter(c => !selected.has(c.name));
        let so: ColumnSortOrientation[] = [];
        for (let i = 0; i < this.order.length(); i++) {
            let cso = this.order.get(i);
            if (!selected.has(cso.columnDescription.name))
                so.push(cso);
        }
        let order = new RecordOrder(so);
        this.schema = schema;
        this.setOrder(order);
    }

    setSchema(schema: IColumnDescription[]): void {
        if (schema == null)
            return;
        this.schema = new SchemaClass(schema);
    }

    // mouse click on a column
    private columnClick(colNum: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey)
            this.selectedColumns.changeState("Ctrl", colNum);
        else if (e.shiftKey)
            this.selectedColumns.changeState("Shift", colNum);
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
            let kind = this.schema.get(i).kind;
            if (kind == "Integer" || kind == "Double") {
                this.selectedColumns.add(i);
                count++;
            }
        }
        this.reportError(`Selected ${count} numeric columns.`);
        this.highlightSelectedColumns();
    }

    private columnClass(colName: string): string {
        let index = this.schema.columnIndex(colName);
        return "col" + String(index);
    }

    public getSelectedColNames(): string[] {
        let colNames: string[] = [];
        this.selectedColumns.getStates().forEach(i => colNames.push(this.schema.get(i).name));
        return colNames;
    }

    private isNumericColumn(colName: string): boolean {
        let kind = this.schema.find(colName).kind;
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
                message += "\n  * Column '" + colName + "' is not numeric.";
            }
        });

        return [valid, message];
    }

    private pca(toSample: boolean): void {
        let colNames = this.getSelectedColNames();
        let [valid, message] = this.checkNumericColumns(colNames);
        if (valid) {
            let pcaDialog = new Dialog("Principal Component Analysis",
                "Projects a set of numeric columns to a smaller set of numeric columns while preserving the 'shape' " +
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
                rr.invoke(new CorrelationMatrixReceiver(this.getPage(), this, rr, this.order,
                    numComponents, projectionName));
            });
            pcaDialog.show();
        } else {
            this.reportError("Not valid for PCA:" + message);
        }
    }

    /*
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
    */

    private highlightSelectedColumns(): void {
        for (let i = 0; i < this.schema.length; i++) {
            let name = this.schema.get(i).name;
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

    public getRowCount() : number {
        return this.tBody.childNodes.length;
    }

    public getColumnCount(): number {
        return this.schema.length;
    }

    public viewSchema(): void {
        let newPage = new FullPage("Schema of ", "Schema", this.page);
        let sv = new SchemaView(this.remoteObjectId, this.dataset,
            newPage, this.schema, this.rowCount, 0);
        newPage.setDataView(sv);
        this.page.insertAfterMe(newPage);
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

    public addRow(row: RowSnapshot, cds: IColumnDescription[]): void {
        let trow = this.tBody.insertRow();

        let position = this.startPosition + this.dataRowsDisplayed;

        let cell = trow.insertCell(0);
        let dataRange = new DataRange(position, row.count, this.rowCount);
        cell.appendChild(dataRange.getDOMRepresentation());

        cell = trow.insertCell(1);
        cell.style.textAlign = "right";
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

                let cellValue: string;
                if (value == null) {
                    cell.classList.add("missingData");
                    cellValue = "missing";
                } else {
                    cellValue = TableView.convert(row.values[dataIndex], cd.kind);
                    value = cellValue;
                }
                cell.textContent = cellValue;
                cell.title = "Right click will popup a menu.";
                cell.oncontextmenu = e => {
                    this.contextMenu.clear();
                    // This menu shows the value to the right, but the filter
                    // takes the value to the left, so we have to flip all
                    // comparison signs.
                    this.contextMenu.addItem({text: "Filter for " + cellValue,
                        action: () => this.filterOnValue(cd.name, value, "=="),
                        help: "Keep only the rows that have this value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Filter for different from " + cellValue,
                        action: () => this.filterOnValue(cd.name, value, "!="),
                        help: "Keep only the rows that have a different value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Filter for < " + cellValue,
                        action: () => this.filterOnValue(cd.name, value, ">"),
                        help: "Keep only the rows that have a a smaller value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Filter for > " + cellValue,
                        action: () => this.filterOnValue(cd.name, value, "<"),
                        help: "Keep only the rows that have a larger value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Filter for <= " + cellValue,
                        action: () => this.filterOnValue(cd.name, value, ">="),
                        help: "Keep only the rows that have a smaller or equal value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Filter for >= " + cellValue,
                        action: () => this.filterOnValue(cd.name, value, "<="),
                        help: "Keep only the rows that have a larger or equal in this column."
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

    public setScroll(top: number, bottom: number): void {
        this.scrollBar.setPosition(top, bottom);
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
                protected order: RecordOrder,
                protected foundCount: number) {
        super(page, operation, "Getting table info");
    }

    onNext(value: PartialResult<NextKList>): void {
        super.onNext(value);
        this.table.updateView(value.data, this.reverse, this.order,
            this.foundCount, this.elapsedMilliseconds());
    }
}

/**
 * Receives the ID for a remote table and initiates a request to get the
 * table schema.
 */
export class RemoteTableReceiver extends RemoteTableRenderer {
    /**
     * Create a renderer for a new table.
     * @param page            Parent page initiating this request.
     * @param operation       Operation that will bring the results.
     * @param title           Title to use for resulting page; if null the parent page is used.
     * @param progressInfo    Description of the files that are being loaded.
     * @param forceTableView  If true the resulting view is always a table.
     */
    constructor(page: FullPage, operation: ICancellable, protected title: string,
                progressInfo: string, protected forceTableView: boolean) {
        super(page, operation, progressInfo, null);
    }

    public run(): void {
        super.run();
        let rr = this.remoteObject.createGetSchemaRequest();
        rr.chain(this.operation);
        let dataset = new Dataset(this.remoteObject.remoteObjectId);
        rr.invoke(new SchemaReceiver(this.page, rr, this.remoteObject, dataset,
            this.title, this.forceTableView));
    }
}

/**
 * Receives a Schema and displays the resulting table.
 */
class SchemaReceiver extends OnCompleteRenderer<TableSummary> {
    /**
     * Create a schema receiver for a new table.
     * @param page            Parent page initiating this request.
     * @param operation       Operation that will bring the results.
     * @param remoteObject    Table object.
     * @param dataset         Dataset that this is a part of.
     * @param title           Title to use for resulting page; if null the parent page is used.
     * @param forceTableView  If true the resulting view is always a table.
     */
    constructor(page: FullPage, operation: ICancellable,
                protected remoteObject: RemoteTableObject,
                protected dataset: Dataset,
                protected title: string, protected forceTableView) {
        super(page, operation, "Get schema")
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
                this.dataset, page, new SchemaClass(summary.schema),
                summary.rowCount, this.elapsedMilliseconds());
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
                this.remoteObject.remoteObjectId, this.dataset, page);
            table.updateView(nk, false, order, null, this.elapsedMilliseconds());
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
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order, null));
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
        super(page, operation, "Correlation matrix", tv.dataset);
    }

    run(): void {
        super.run();
        let rr = this.tv.createProjectToEigenVectorsRequest(
                this.remoteObject, this.numComponents, this.projectionName);
        rr.chain(this.operation);
        rr.invoke(new PCATableReceiver(
            this.page, rr, "Data with PCA projection columns", "Reading", this.tv, this.order, this.numComponents));
    }
}

// Receives the ID of a table that contains additional eigen vector projection columns.
// Invokes a sketch to get the schema of this new table.
class PCATableReceiver extends RemoteTableRenderer {
    constructor(page: FullPage, operation: ICancellable, protected title: string, progressInfo: string,
                protected tv: TableViewBase, protected order: RecordOrder, protected numComponents: number) {
        super(page, operation, progressInfo, tv.dataset);
    }

    public run(): void {
        super.run();
        let rr = this.remoteObject.createGetSchemaRequest();
        rr.chain(this.operation);
        rr.invoke(new PCASchemaReceiver(this.page, rr, this.remoteObject, this.tv,
            this.title, this.order, this.numComponents));
    }
}

// Receives the schema after a PCA computation; computes the additional columns
// and adds these to the previous view
class PCASchemaReceiver extends OnCompleteRenderer<TableSummary> {
    constructor(page: FullPage, operation: ICancellable,
                protected remoteObject: RemoteTableObject,
                protected tv: TableViewBase,
                protected title: string,
                protected order: RecordOrder,
                protected numComponents: number) {
        super(page, operation, "Get schema")
    }

    run(summary: TableSummary): void {
        if (summary.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }

        let newCols: IColumnDescription[] = [];
        let o = this.order.clone();
        // we rely on the fact that the last numComponents columns are added by the PCA
        // computation.
        for (let i = 0; i < this.numComponents; i++) {
            let cd = summary.schema[summary.schema.length - this.numComponents + i];
            newCols.push(cd);
            o.addColumn({ columnDescription: cd, isAscending: true });
        }

        let table = new TableView(
            this.remoteObject.remoteObjectId, this.tv.dataset, this.page);
        table.schema = this.tv.schema.concat(newCols);
        this.page.setDataView(table);
        let rr = table.createNextKRequest(o, null);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, table, rr, false, o, null));
    }
}

/**
 * Receives the id of a remote table and
 * initiates a request to display the nextK rows from this table.
 */
export class TableOperationCompleted extends RemoteTableRenderer {
    public constructor(page: FullPage,
                       protected schema: SchemaClass,
                       operation: ICancellable,
                       protected order: RecordOrder,
                       dataset: Dataset) {
        super(page, operation, "Table operation", dataset);
    }

    run(): void {
        super.run();
        let table = new TableView(
            this.remoteObject.remoteObjectId, this.dataset, this.page);
        table.schema = this.schema;
        this.page.setDataView(table);
        let rr = table.createNextKRequest(this.order, null);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, table, rr, false, this.order, null));
    }
}

/**
 * Receives a result from a remote table and initiates a NextK sketch
 * if any result is found.
 */
export class FindReceiver extends OnCompleteRenderer<FindResult> {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable,
                       protected order: RecordOrder) {
        super(page, operation, "Compute quantiles");
    }

    run(result: FindResult): void {
        if (result.count == 0) {
            this.page.reportError("No matches found");
            return;
        }
        let rr = this.tv.createNextKRequest(this.order, result.firstRow);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order, result.count));
    }
}
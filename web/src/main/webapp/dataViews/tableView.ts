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

import {DatasetView, IViewSerialization, TableSerialization} from "../datasetView";
import {
    asContentsKind,
    ColumnSortOrientation,
    Comparison,
    ComparisonFilterDescription,
    ContentsKind,
    FindResult,
    IColumnDescription,
    kindIsString,
    NextKList,
    RecordOrder,
    RemoteObjectId,
    RowSnapshot,
    Schema,
    StringFilterDescription,
    TableSummary
} from "../javaBridge";
import {OnCompleteReceiver, Receiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, TableTargetAPI} from "../tableTarget";
import {DataRangeUI} from "../ui/dataRangeUI";
import {IDataView} from "../ui/dataview";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {ContextMenu, SubMenu, TopMenu} from "../ui/menu";
import {IScrollTarget, ScrollBar} from "../ui/scroll";
import {SelectionStateMachine} from "../ui/selectionStateMachine";
import {HtmlString, Resolution, SpecialChars} from "../ui/ui";
import {
    cloneToSet, convertToStringFormat,
    formatNumber,
    ICancellable, makeMissing, makeSpan,
    PartialResult,
    percent,
    saveAs,
    significantDigitsHtml, truncate,
} from "../util";
import {SchemaView} from "./schemaView";
import {SpectrumReceiver} from "./spectrumView";
import {ColumnConverter, ConverterDialog, TSViewBase} from "./tsViewBase";
import {CountSketchReceiver} from "./tsViewBase";
import {Grid} from "../ui/grid";

/**
 * Displays a table in the browser.
 */
export class TableView extends TSViewBase implements IScrollTarget {
    // Data view part: received from remote site
    // Logical position of first row displayed
    protected startPosition?: number;
    public    order: RecordOrder;
    // Logical number of data rows displayed; includes count of each data row
    protected dataRowsDisplayed: number;
    public    tableRowsDesired: number;
    protected scrollBar: ScrollBar;
    protected grid: Grid;
    protected nextKList: NextKList;
    protected contextMenu: ContextMenu;
    protected cellsPerColumn: Map<string, HTMLElement[]>;
    protected selectedColumns = new SelectionStateMachine();
    protected message: HTMLElement;

    // The following elements are used for Find
    protected strFilter: StringFilterDescription;
    protected findInputBox: HTMLInputElement;
    protected substringsFindCheckbox: HTMLInputElement;
    protected regexFindCheckbox: HTMLInputElement;
    protected caseFindCheckbox: HTMLInputElement;
    protected findBar: HTMLElement;
    protected findBarVisible: boolean;
    protected foundCount: HTMLElement;

    public constructor(
        remoteObjectId: RemoteObjectId, rowCount: number, schema: SchemaClass, page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Table");
        this.selectedColumns = new SelectionStateMachine();
        this.tableRowsDesired = Resolution.tableRowsOnScreen;
        this.order = new RecordOrder([]);
        this.topLevel = document.createElement("div");
        this.topLevel.id = "tableContainer";
        this.topLevel.tabIndex = 1;  // necessary for keyboard events?
        this.topLevel.onkeydown = (e) => this.keyDown(e);
        this.strFilter = null;

        const menu = new TopMenu([
            {
                text: "Export",
                help: "Save information from this view in a local file.",
                subMenu: new SubMenu([{
                    text: "Schema",
                    help: "Saves the schema of this data JSON file.",
                    action: () => { this.exportSchema(); },
                }]),
            },
            // this.saveAsMenu(),
            {
                text: "View", help: "Change the way the data is displayed.", subMenu: new SubMenu([
                    { text: "Refresh",
                        action: () => this.refresh(),
                        help: "Redraw this view.",
                    },
                    /*
                    { text: "All columns",
                        action: () => this.showAllColumns(),
                        help: "Make all columns visible."
                    },
                    */
                    { text: "No columns",
                        action: () => this.setOrder(new RecordOrder([])),
                        help: "Make all columns invisible",
                    },
                    { text: "Schema",
                        action: () => this.viewSchema(),
                        help: "Browse the list of columns of this table and choose a subset to visualize.",
                    },
                    {
                        text: "Change table size...",
                        action: () => this.changeTableSize(),
                        help: "Change the number of rows displayed",
                    },
                ]),
            },
            this.chartMenu(),
            {
                text: "Filter", help: "Search specific values",
                subMenu: new SubMenu([
                    {
                        text: "Find...",
                        help: "Search for a string in the visible columns",
                        action: () => {
                            if (this.order.length() === 0) {
                                this.page.reportError(
                                    "Find operates in the displayed column, " +
                                    "but no column is currently visible.");
                                return;
                            }
                            this.showFindBar(true);
                        }
                    },
                    {
                        text: "Filter...",
                        help: "Filter rows that contain a specific value",
                        action: () => this.showFilterDialog(null, this.order, this.tableRowsDesired) },
                    {
                        text: "Compare...",
                        help: "Filter rows by comparing with a specific value",
                        action: () => this.showCompareDialog(null, this.order, this.tableRowsDesired) },
                ]),
            },
            this.dataset.combineMenu(this, page.pageId),
        ]);

        this.page.setMenu(menu);
        this.contextMenu = new ContextMenu(this.topLevel);
        this.topLevel.appendChild(document.createElement("hr"));
        this.scrollBar = new ScrollBar(this, false);

        // to force the scroll bar next to the table we put them in yet another div
        const tblAndScrollBar = document.createElement("div");
        tblAndScrollBar.style.flexDirection = "row";
        tblAndScrollBar.style.display = "flex";
        tblAndScrollBar.style.flexWrap = "nowrap";
        tblAndScrollBar.style.justifyContent = "flex-start";
        tblAndScrollBar.style.alignItems = "stretch";
        this.topLevel.appendChild(tblAndScrollBar);
        this.grid = new Grid(80);
        tblAndScrollBar.appendChild(this.scrollBar.getHTMLRepresentation());
        tblAndScrollBar.appendChild(this.grid.getHTMLRepresentation());
        this.initFindBar();

        this.message = document.createElement("div");
        this.topLevel.appendChild(this.message);
    }

    private exportSchema(): void {
        saveAs("schema.json", JSON.stringify(this.schema.schema));
    }

    private showFindBar(show: boolean): void {
        if (show) {
            this.findBar.style.display = "flex";
            this.findInputBox.focus();
            this.findBarVisible = true;
        } else {
            this.findBar.style.display = "none";
            this.findBarVisible = false;
        }
    }

    private addSpace(num: number): void {
        const span = makeSpan("", false);
        let str: string = "";
        for (let i = 0; i < num; i++)
            str += "&nbsp;";
        span.innerHTML = str;
        this.findBar.appendChild(span);
    }

    private addCheckbox(stringDesc: string ): HTMLInputElement {
        this.addSpace(2);
        const label = document.createElement("label");
        label.textContent = stringDesc;
        this.findBar.appendChild(label);
        const checkBox: HTMLInputElement = document.createElement("input");
        checkBox.type = "checkbox";
        this.findBar.appendChild(checkBox);
        const divEnd: HTMLElement = document.createElement("div");
        divEnd.innerHTML = "&nbsp;&nbsp;";
        this.findBar.appendChild(divEnd);
        return checkBox;
    }

    private initFindBar(): void {
        this.findBarVisible = false;
        this.findBar = document.createElement("div");
        this.findBar.style.margin = "2px";
        this.findBar.className = "highlight";
        this.findBar.style.flexDirection = "row";
        this.findBar.style.display = "none";
        this.findBar.style.flexWrap = "nowrap";
        this.findBar.style.justifyContent = "flex-start";
        this.findBar.style.alignItems = "center";

        const label = document.createElement("label");
        label.innerText = "Find: ";
        this.findBar.appendChild(label);

        this.findInputBox = document.createElement("input");
        this.findBar.appendChild(this.findInputBox);
        this.addSpace(1);

        const nextButton = this.findBar.appendChild(document.createElement("button"));
        nextButton.innerHTML = SpecialChars.downArrowHtml;
        nextButton.onclick = () => this.find(true, false);
        this.addSpace(1);

        const prevButton = this.findBar.appendChild(document.createElement("button"));
        prevButton.innerHTML = SpecialChars.upArrowHtml;
        prevButton.onclick = () => this.find(false, false);
        this.addSpace(1);

        const topButton = this.findBar.appendChild(document.createElement("button"));
        topButton.innerText = "Search from top";
        topButton.onclick = () => this.find(true, true);
        this.addSpace(1);

        this.substringsFindCheckbox = this.addCheckbox("Substrings:");
        this.regexFindCheckbox = this.addCheckbox("Regex:");
        this.caseFindCheckbox = this.addCheckbox("Match case:");

        this.foundCount = document.createElement("div");
        this.findBar.appendChild(this.foundCount);

        const filler = document.createElement("div");
        filler.style.flexGrow = "100";
        this.findBar.appendChild(filler);

        const close = document.createElement("span");
        close.className = "close";
        close.innerHTML = "&times;";
        close.onclick = () => this.showFindBar(false);
        close.title = "Cancel the find.";
        this.findBar.appendChild(close);

        this.topLevel.appendChild(this.findBar);
    }

    public serialize(): IViewSerialization {
        const result: TableSerialization = {
            ...super.serialize(),
            order: this.order,
            tableRowsDesired: this.tableRowsDesired,
            firstRow: this.nextKList.rows.length > 0 ? this.nextKList.rows[0].values : null,
        };
        return result;
    }

    public static reconstruct(ser: TableSerialization | null, page: FullPage): IDataView {
        const order = new RecordOrder(ser.order.sortOrientationList);
        const firstRow: any[] = ser.firstRow;
        const schema = new SchemaClass([]).deserialize(ser.schema);
        const rowsDesired = ser.tableRowsDesired;
        if (order == null || schema == null || rowsDesired == null)
            return null;
        const tableView = new TableView(ser.remoteObjectId, ser.rowCount, schema, page);
        // We need to set the first row for the refresh.
        tableView.nextKList = {
            rowsScanned: 0,
            rows: firstRow != null ?
                [ { count: 0, values: firstRow } ] : null,
            startPosition: 0  // not used
        };
        tableView.order = order;
        tableView.tableRowsDesired = rowsDesired;
        return tableView;
    }

    private static compareFilters(a: StringFilterDescription, b: StringFilterDescription): boolean {
        if ((a == null) || (b == null))
            return ((a == null) && (b == null));
        else
            return ((a.compareValue === b.compareValue) &&
                (a.asRegEx === b.asRegEx) &&
                (a.asSubString === b.asSubString) &&
                (a.caseSensitive === b.caseSensitive) &&
                (a.complement === b.complement));
    }

    private find(next: boolean, fromTop: boolean): void {
        if (this.order.length() === 0) {
            this.page.reportError("Find operates in the displayed column, but no column is currently visible.");
            return;
        }
        if (this.nextKList.rows.length === 0) {
            this.page.reportError("No data to search in");
            return;
        }
        let excludeTopRow: boolean;

        const newFilter: StringFilterDescription = {
            compareValue: this.findInputBox.value,
            asRegEx: this.regexFindCheckbox.checked,
            asSubString: this.substringsFindCheckbox.checked,
            caseSensitive: this.caseFindCheckbox.checked,
            complement: false
        };
        if (TableView.compareFilters(this.strFilter, newFilter)) {
            excludeTopRow = true; // next search
        } else {
            this.strFilter = newFilter;
            excludeTopRow = false; // new search
        }
        if (!next)
            excludeTopRow = true;
        if (this.strFilter.compareValue === "") {
            this.page.reportError("No current search string.");
            return;
        }
        const o = this.order.clone();
        const topRow: any[] = (fromTop ? null : this.nextKList.rows[0].values);
        const rr = this.createFindRequest(o, topRow, this.strFilter, excludeTopRow, next);
        rr.invoke(new FindReceiver(this.getPage(), rr, this, o));
    }

    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseRenderer {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new TableOperationCompleted(page, operation, this.rowCount, this.schema,
                this.order.clone(), this.tableRowsDesired);
        };
    }

    public getSelectedColCount(): number {
        return this.selectedColumns.size();
    }

    /**
     * Invoked when scrolling has completed.
     */
    public scrolledTo(position: number): void {
        if (this.nextKList == null)
            return;

        if (position <= 0) {
            this.begin();
        } else if (position >= 1.0) {
            this.end();
        } else {
            const o = this.order.clone();
            const rr = this.createQuantileRequest(this.rowCount, o, position);
            console.log("expecting quantile: " + String(position));
            rr.invoke(new QuantileReceiver(this.getPage(), this, rr, o));
        }
    }

    /**
     * Event handler called when a key is pressed
     */
    protected keyDown(ev: KeyboardEvent): void {
        if (ev.code === "PageUp") {
            this.pageUp();
            ev.preventDefault();
        } else if (ev.code === "PageDown") {
            this.pageDown();
            ev.preventDefault();
        } else if (ev.code === "End") {
            this.end();
            ev.preventDefault();
        } else if (ev.code === "Home") {
            this.begin();
            ev.preventDefault();
        }
    }

    /**
     * Scroll one page up
     */
    public pageUp(): void {
        if (this.nextKList == null || this.nextKList.rows.length === 0)
            return;
        if (this.startPosition <= 0) {
            this.page.reportError("Already at the top");
            return;
        }
        const order = this.order.invert();
        const rr = this.createNextKRequest(order, this.nextKList.rows[0].values, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, true, order, null));
    }

    protected begin(): void {
        if (this.nextKList == null || this.nextKList.rows.length === 0)
            return;
        if (this.startPosition <= 0) {
            this.page.reportError("Already at the top");
            return;
        }
        const o = this.order.clone();
        const rr = this.createNextKRequest(o, null, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    protected end(): void {
        if (this.nextKList == null || this.nextKList.rows.length === 0)
            return;
        if (this.startPosition + this.dataRowsDisplayed >= this.rowCount - 1) {
            this.page.reportError("Already at the bottom");
            return;
        }
        const order = this.order.invert();
        const rr = this.createNextKRequest(order, null, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, true, order, null));
    }

    public pageDown(): void {
        if (this.nextKList == null || this.nextKList.rows.length === 0)
            return;
        if (this.startPosition + this.dataRowsDisplayed >= this.rowCount - 1) {
            this.page.reportError("Already at the bottom");
            return;
        }
        const o = this.order.clone();
        const rr = this.createNextKRequest(
            o, this.nextKList.rows[this.nextKList.rows.length - 1].values, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    protected setOrder(o: RecordOrder): void {
        const rr = this.createNextKRequest(o, null, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    protected showAllColumns(): void {
        if (this.schema == null) {
            this.page.reportError("No data loaded");
            return;
        }

        const o = this.order.clone();
        for (let i = 0; i < this.schema.length; i++) {
            const c = this.schema.get(i);
            o.addColumnIfNotVisible({ columnDescription: c, isAscending: true });
        }
        this.setOrder(o);
    }

    public getSortOrder(column: string): [boolean, number] {
        for (let i = 0; i < this.order.length(); i++) {
            const o = this.order.get(i);
            if (o.columnDescription.name === column)
                return [o.isAscending, i];
        }
        return null;
    }

    public isVisible(column: string): boolean {
        const so = this.getSortOrder(column);
        return so != null;
    }

    public isAscending(column: string): boolean {
        const so = this.getSortOrder(column);
        if (so == null) return null;
        return so[0];
    }

    public getSortIndex(column: string): number {
        const so = this.getSortOrder(column);
        if (so == null) return null;
        return so[1];
    }

    public getSortArrow(column: string): string {
        const asc = this.isAscending(column);
        if (asc == null)
            return "";
        else if (asc)
            return SpecialChars.downArrow;
        else
            return SpecialChars.upArrow;
    }

    private addHeaderCell(cd: IColumnDescription,
                          displayName: string,
                          help: string,
                          width: number): HTMLElement {
        const th = this.grid.addHeader(width, cd.name);
        th.classList.add("noselect");
        th.title = help;
        if (!this.isVisible(cd.name)) {
            th.style.fontWeight = "normal";
        } else {
            const span = makeSpan("", false);
            span.innerHTML = this.getSortIndex(cd.name) + this.getSortArrow(cd.name);
            span.style.cursor = "pointer";
            span.onclick = () => this.toggleOrder(cd.name);
            th.appendChild(span);
        }
        th.appendChild(makeSpan(displayName, false));
        return th;
    }

    protected toggleOrder(colName: string): void {
        const o = this.order.toggle(colName);
        this.setOrder(o);
    }

    public showColumns(order: number, first: boolean): void {
        // order is 0 to hide
        //         -1 to sort descending
        //          1 to sort ascending
        const o = this.order.clone();
        // The set iterator did not seem to work correctly...
        this.getSelectedColNames().forEach((colName) => {
            const col = this.schema.find(colName);
            if (order !== 0 && col != null) {
                if (first)
                    o.sortFirst({columnDescription: col, isAscending: order > 0});
                else
                    o.addColumn({columnDescription: col, isAscending: order > 0});
            } else
                o.hide(colName);
        });
        this.setOrder(o);
    }

    public resize(): void {
        this.updateView(this.nextKList, false, this.order, null, 0);
    }

    public refresh(): void {
        if (this.nextKList == null) {
            this.page.reportError("Nothing to refresh");
            return;
        }

        let firstRow = null;
        if (this.nextKList.rows != null &&
            this.nextKList.rows.length > 0)
            firstRow = this.nextKList.rows[0].values;
        const rr = this.createNextKRequest(this.order, firstRow, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.page, this, rr, false, this.order, null));
    }

    public updateView(nextKList: NextKList,
                      revert: boolean,
                      order: RecordOrder,
                      result: FindResult,
                      elapsedMs: number): void {
        this.grid.prepareForUpdate();
        this.selectedColumns.clear();
        this.rowCount = nextKList.rowsScanned;
        this.nextKList = nextKList;
        this.dataRowsDisplayed = 0;
        this.startPosition = nextKList.startPosition;
        this.order = order.clone();

        if (revert) {
            let rowsDisplayed = 0;
            if (nextKList.rows != null) {
                nextKList.rows.reverse();
                rowsDisplayed = nextKList.rows.map((r) => r.count).reduce((a, b) => a + b, 0);
            }
            this.startPosition = this.rowCount - this.startPosition - rowsDisplayed;
            this.order = this.order.invert();
        }

        // These two columns are always shown
        const cds: IColumnDescription[] = [];
        const posCd: IColumnDescription = {
            kind: "Integer",
            name: "(position)",
        };
        const ctCd: IColumnDescription = {
            kind: "Integer",
            name: "(count)",
        };

        {
            // Create column headers
            let thd = this.addHeaderCell(posCd, posCd.name, "Position within sorted order.", DataRangeUI.width);
            thd.oncontextmenu = () => {};
            thd = this.addHeaderCell(ctCd, ctCd.name, "Number of occurrences.", 75);
            thd.oncontextmenu = () => {};
            if (this.schema == null)
                return;
        }

        for (let i = 0; i < this.schema.length; i++) {
            const cd = this.schema.get(i);
            cds.push(cd);

            const kindString = cd.kind;
            const name = this.schema.displayName(cd.name);
            const title = name + ".\nType is " + kindString +
                ".\nRight mouse click opens a menu.";
            const thd = this.addHeaderCell(cd, name, title, 0);
            thd.classList.add("col" + i.toString());
            thd.onclick = (e) => this.columnClick(i, e);
            thd.oncontextmenu = (e) => {
                this.columnClick(i, e);
                if (e.ctrlKey && (e.button === 1)) {
                    // Ctrl + click is interpreted as a right-click on macOS.
                    // This makes sure it's interpreted as a column click with Ctrl.
                    return;
                }

                const selectedCount = this.selectedColumns.size();
                this.contextMenu.clear();
                if (this.order.find(cd.name) >= 0) {
                    this.contextMenu.addItem({
                        text: "Hide",
                        action: () => this.showColumns(0, true),
                        help: "Hide the data in the selected columns",
                    }, true);
                } else {
                    this.contextMenu.addItem({
                        text: "Show",
                        action: () => this.showColumns(1, false),
                        help: "Show the data in the selected columns.",
                    }, true);
                }

                this.contextMenu.addItem({
                    text: "Drop",
                    action: () => this.dropColumns(),
                    help: "Eliminate the selected columns from the view.",
                }, selectedCount !== 0);
                this.contextMenu.addItem({
                    text: "Estimate distinct elements",
                    action: () => this.hLogLog(),
                    help: "Compute an estimate of the number of different values that appear in the selected column.",
                }, selectedCount === 1);
                this.contextMenu.addItem({
                    text: "Sort ascending",
                    action: () => this.showColumns(1, true),
                    help: "Sort the data first on this colum, in increasing order.",
                }, true);
                this.contextMenu.addItem({
                    text: "Sort descending",
                    action: () => this.showColumns(-1, true),
                    help: "Sort the data first on this column, in decreasing order",
                }, true);
                this.contextMenu.addItem({
                    text: "Histogram",
                    action: () => this.histogramSelected(),
                    help: "Plot the data in the selected columns as a histogram. " +
                    "Applies to one or two columns only.",
                }, selectedCount >= 1 && selectedCount <= 2);
                this.contextMenu.addItem({
                    text: "Heatmap",
                    action: () => this.heatmapSelected(),
                    help: "Plot the data in the selected columns as a heatmap. " +
                    "Applies to two columns only.",
                }, selectedCount === 2);
                this.contextMenu.addItem({
                    text: "Trellis histograms",
                    action: () => this.trellisSelected(false),
                    help: "Plot the data in the selected columns as a Trellis plot of histograms. " +
                    "Applies to two or three columns only.",
                }, selectedCount >= 2 && selectedCount <= 3);
                this.contextMenu.addItem({
                    text: "Trellis heatmaps",
                    action: () => this.trellisSelected(true),
                    help: "Plot the data in the selected columns as a Trellis plot of heatmaps. " +
                    "Applies to three columns only.",
                }, selectedCount === 3);
                this.contextMenu.addItem({
                    text: "Rename...",
                    action: () => this.renameColumn(),
                    help: "Give a new name to this column.",
                }, selectedCount === 1);
                this.contextMenu.addItem({
                    text: "Frequent Elements...",
                    action: () => this.heavyHittersDialog(),
                    help: "Find the values that occur most frequently in the selected columns.",
                }, true);
                /*
                this.contextMenu.addItem({
                    text: "Count Sketch",
                    action: () => this.runCountSketch(),
                    help: "Find the values that occur most frequently in the selected columns.",
                }, true);
                */
                this.contextMenu.addItem({
                    text: "PCA...",
                    action: () => this.pca(true),
                    help: "Perform Principal Component Analysis on a set of numeric columns. " +
                    "This produces a smaller set of columns that preserve interesting properties of the data.",
                }, selectedCount > 1 &&
                    this.getSelectedColNames().reduce((a, b) => a && this.isNumericColumn(b), true));
                this.contextMenu.addItem({
                    text: "Plot Singular Value Spectrum",
                    action: () => this.spectrum(true),
                    help: "Plot singular values for the selected columns. ",
                }, selectedCount > 1 &&
                    this.getSelectedColNames().reduce((a, b) => a && this.isNumericColumn(b), true));
                this.contextMenu.addItem({
                    text: "Filter...",
                    action: () => {
                        const colName = this.getSelectedColNames()[0];
                        const colDesc = this.schema.find(colName);
                        this.showFilterDialog(colDesc.name, this.order, this.tableRowsDesired);
                    },
                    help: "Eliminate data that matches/does not match a specific value.",
                }, selectedCount === 1);
                this.contextMenu.addItem({
                    text: "Compare...",
                    action: () => {
                        const colName = this.getSelectedColNames()[0];
                        this.showCompareDialog(this.schema.displayName(colName),
                            this.order, this.tableRowsDesired);
                    },
                    help: "Eliminate data that matches/does not match a specific value.",
                }, selectedCount === 1);
                this.contextMenu.addItem({
                    text: "Convert...",
                    action: () => this.convert(cd.name),
                    help: "Convert the data in the selected column to a different data type.",
                }, selectedCount === 1);
                this.contextMenu.addItem({
                    text: "Create column...",
                    action: () => this.createColumnDialog(this.order, this.tableRowsDesired),
                    help: "Add a new column computed from the selected columns.",
                }, true);
                this.contextMenu.show(e);
            };
        }

        this.cellsPerColumn = new Map<string, HTMLElement[]>();
        cds.forEach((cd) => this.cellsPerColumn.set(cd.name, []));
        let tableRowCount = 0;
        // Add row data
        let previousRow: RowSnapshot = null;
        if (nextKList.rows != null) {
            tableRowCount = nextKList.rows.length;
            let index = 0;
            for (const row of nextKList.rows) {
                this.addRow(row, previousRow, cds, index === nextKList.rows.length - 1);
                previousRow = row;
                index++;
            }
        }

        let perc = "";
        if (this.rowCount > 0)
            perc = percent(this.dataRowsDisplayed / this.rowCount);
        if (this.startPosition > 0) {
            if (perc !== "")
                perc += " ";
            perc += "starting at " + percent(this.startPosition / this.rowCount);
        }
        if (perc !== "")
            perc = " (" + perc + ")";

        const message = new HtmlString(tableRowCount + " displayed rows represent " +
            formatNumber(this.dataRowsDisplayed) +
            "/" + formatNumber(this.rowCount) + " data rows" + perc);
        message.setInnerHtml(this.message);

        if (result != null) {
            this.foundCount.textContent = formatNumber(result.before) + " matching before, " +
                formatNumber(result.after) + " after";
        } else {
            this.strFilter = null;
            this.showFindBar(false);
        }

        this.updateScrollBar();
        this.highlightSelectedColumns();
        this.page.reportTime(elapsedMs);
        this.grid.updateCompleted();
    }

    public filterOnValue(cd: IColumnDescription, value: string | number, comparison: Comparison): void {
        const cfd: ComparisonFilterDescription = {
            column: cd,
            stringValue: kindIsString(cd.kind) ? value as string : null,
            doubleValue: !kindIsString(cd.kind) ? value as number : null,
            comparison,
        };
        this.runComparisonFilter(cfd, this.order, this.tableRowsDesired);
    }

    /**
     * Convert the data in a column to a different column kind.
     */
    public convert(colName: string): void {
        const cd = new ConverterDialog(colName, this.schema.columnNames);
        cd.setAction(
            () => {
                const kindStr = cd.getFieldValue("newKind");
                const kind: ContentsKind = asContentsKind(kindStr);
                const col = cd.getFieldValue("columnName");
                const keep = cd.getBooleanValue("keep");
                const newColName = keep ?
                    cd.getFieldValue("newColumnName") :
                    cd.generateFreshName(col, "");
                const converter = new ColumnConverter(
                    col, kind, newColName, keep, this,
                    this.order, this.page);
                converter.run();
            });
        cd.show();
    }

    public dropColumns(): void {
        const selected = cloneToSet(this.getSelectedColNames());
        const schema = this.schema.filter((c) => !selected.has(c.name));
        const so: ColumnSortOrientation[] = [];
        for (let i = 0; i < this.order.length(); i++) {
            const cso = this.order.get(i);
            if (!selected.has(cso.columnDescription.name))
                so.push(cso);
        }
        const order = new RecordOrder(so);
        this.schema = schema;
        this.setOrder(order);
    }

    // mouse click on a column
    private columnClick(colNum: number, e: MouseEvent): void {
        e.preventDefault();
        if (e.ctrlKey || e.metaKey)
            this.selectedColumns.changeState("Ctrl", colNum);
        else if (e.shiftKey)
            this.selectedColumns.changeState("Shift", colNum);
        else {
            if (e.button === 2) {
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
            const kind = this.schema.get(i).kind;
            if (kind === "Integer" || kind === "Double") {
                this.selectedColumns.add(i);
                count++;
            }
        }
        this.page.reportError(`Selected ${count} numeric columns.`);
        this.highlightSelectedColumns();
    }

    public getSelectedColNames(): string[] {
        const colNames: string[] = [];
        this.selectedColumns.getStates().forEach((i) => colNames.push(this.schema.get(i).name));
        return colNames;
    }

    private isNumericColumn(colName: string): boolean {
        const kind = this.schema.find(colName).kind;
        return kind === "Double" || kind === "Integer";
    }

    private checkNumericColumns(colNames: string[], atLeast: number = 3): [boolean, string] {
        if (colNames.length < atLeast) {
            const msg = `\nNot enough columns. Need at least ${atLeast}. There are ${colNames.length}`;
            return [false, msg];
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

    public pca(toSample: boolean): void {
        const colNames = this.getSelectedColNames();
        const [valid, message] = this.checkNumericColumns(colNames, 2);
        if (valid) {
            const pcaDialog = new Dialog("Principal Component Analysis",
                "Projects a set of numeric columns to a smaller set of numeric columns while preserving the 'shape' " +
                " of the data as much as possible.");
            const components = pcaDialog.addTextField("numComponents", "Number of components",
                FieldKind.Integer, "2",
                "Number of dimensions to project to.  Must be an integer bigger than 1 and " +
                "smaller than the number of selected columns");
            components.required = true;
            components.min = "2";
            components.max = colNames.length.toString();
            const name = pcaDialog.addTextField("projectionName", "Name for Projected columns", FieldKind.String,
                "PCA",
                "The projected columns will appear with this name followed by a number starting from 0");
            name.required = true;
            pcaDialog.setCacheTitle("PCADialog");
            pcaDialog.setAction(() => {
                const numComponents: number = pcaDialog.getFieldValueAsInt("numComponents");
                const projectionName: string = pcaDialog.getFieldValue("projectionName");
                if (numComponents < 1 || numComponents > colNames.length) {
                    this.page.reportError("Number of components for PCA must be between 1 (incl.) " +
                        "and the number of selected columns, " + colNames.length + " (incl.). (" +
                        numComponents + " does not satisfy this.)");
                    return;
                }
                const rr = this.createCorrelationMatrixRequest(colNames, this.rowCount, toSample);
                rr.invoke(new CorrelationMatrixReceiver(this.getPage(), this, rr, this.order,
                    numComponents, projectionName));
            });
            pcaDialog.show();
        } else {
            this.page.reportError("Not valid for PCA:" + message);
        }
    }

    protected runCountSketch(): void {
        const columnsShown: IColumnDescription[] = [];
        const cso: ColumnSortOrientation[] = [];
        this.getSelectedColNames().forEach((v) => {
            const colDesc = this.schema.find(v);
            columnsShown.push(colDesc);
            cso.push({columnDescription: colDesc, isAscending: true});
        });
        const order = new RecordOrder(cso);
        const rr = this.createCountSketchRequest(columnsShown);
        rr.invoke(new CountSketchReceiver(this.getPage(), this, rr, this.rowCount,
            this.schema, columnsShown, order));
    }

    private spectrum(toSample: boolean): void {
        const colNames = this.getSelectedColNames();
        const [valid, message] = this.checkNumericColumns(colNames, 2);
        if (valid) {
            const rr = this.createSpectrumRequest(colNames, this.rowCount, toSample);
            rr.invoke(new SpectrumReceiver(
                this.getPage(), this, this.remoteObjectId, this.rowCount,
                this.schema, colNames, rr, false));
        } else {
            this.page.reportError("Not valid for PCA:" + message);
        }
    }

    private highlightSelectedColumns(): void {
        for (let i = 0; i < this.schema.length; i++) {
            const name = this.schema.get(i).name;
            const header = this.grid.getHeader(i + 2);  // 2 extra columns
            const cells = this.cellsPerColumn.get(name);
            const selected = this.selectedColumns.has(i);
            if (selected)
                header.classList.add("selected");
            else
                header.classList.remove("selected");
            for (const cell of cells) {
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

    protected changeTableSize(): void {
        const dialog = new Dialog("Number of rows", "Choose number of rows to display");
        const field = dialog.addTextField("rows", "Rows", FieldKind.Integer,
            Resolution.tableRowsOnScreen.toString(),
            "Number of rows to show (between 10 and 200)");
        field.min = "10";
        field.max = "200";
        field.required = true;
        dialog.setAction(() => {
            const rowCount = dialog.getFieldValueAsInt("rows");
            if (rowCount < 10 || rowCount > 200) {
                this.page.reportError("Row count must be between 10 and 200");
                return;
            }
            this.tableRowsDesired = rowCount;
            this.refresh();
        });
        dialog.show();
    }

    public viewSchema(): void {
        const newPage = this.dataset.newPage(new PageTitle("Schema"), this.page);
        const sv = new SchemaView(this.remoteObjectId, newPage, this.rowCount, this.schema, 0);
        newPage.setDataView(sv);
        newPage.scrollIntoView();
    }

    public moveRowToTop(row: RowSnapshot): void {
        const rr = this.createNextKRequest(this.order, row.values, this.tableRowsDesired);
        rr.invoke(new NextKReceiver(this.page, this, rr, false, this.order, null));
    }

    /**
     * Hilight a table's cell text according to the current find.
     * Returns an HTML element that can be inserted in the table cell as a child.
     */
    private highlight(text: string): HTMLElement {
        if (!this.findBarVisible || this.strFilter == null)
            return makeSpan(text, false);

        const find = this.strFilter.compareValue;
        if (find == null || find === "")
            return makeSpan(text, false);

        const result = makeSpan(null, false);
        if (this.strFilter.asRegEx) {
            const modifier = this.strFilter.caseSensitive ? "g" : "ig";
            let regex = new RegExp(find, modifier);
            if (!this.strFilter.asSubString)
                regex = new RegExp("^" + find + "$", modifier);
            while (true) {
                const match = regex.exec(text);
                if (match == null) {
                    result.appendChild(makeSpan(text, false));
                    return result;
                }
                result.appendChild(makeSpan(text.substr(0, match.index), false));
                result.appendChild(makeSpan(text.substr(match.index, regex.lastIndex - match.index), true));
                text = text.substr(regex.lastIndex);
            }
        } else {
            if (this.strFilter.asSubString) {
                let index: number;
                while (true) {
                    if (this.strFilter.caseSensitive)
                        index = text.indexOf(find);
                    else
                        index = text.toLowerCase().indexOf(find.toLowerCase());
                    if (index < 0) {
                        result.appendChild(makeSpan(text, false));
                        return result;
                    }
                    result.appendChild(makeSpan(text.substr(0, index), false));
                    result.appendChild(makeSpan(text.substr(index, find.length), true));
                    text = text.substr(index + find.length);
                }
            } else {
                if (this.strFilter.caseSensitive) {
                    if (text === find)
                        return makeSpan(text, true);
                } else {
                    if (text.toLowerCase() === find.toLowerCase())
                        return makeSpan(text, true);
                }
            }
        }
        return makeSpan(text, false);
    }

    public addRow(row: RowSnapshot, previousRow: RowSnapshot | null,
                  cds: IColumnDescription[], last: boolean): void {
        this.grid.newRow();
        const position = this.startPosition + this.dataRowsDisplayed;
        const moveToTop = (e: PointerEvent) => {
            this.contextMenu.clear();
            this.contextMenu.addItem({
                text: "Move to top",
                action: () => this.moveRowToTop(row),
                help: "Move this row to the top of the view.",
            }, true);
            this.contextMenu.show(e);
        };

        let cell = this.grid.newCell("all");
        const dataRange = new DataRangeUI(position, row.count, this.rowCount);
        cell.appendChild(dataRange.getDOMRepresentation());
        cell.classList.add("meta");
        cell.oncontextmenu = moveToTop;

        cell = this.grid.newCell("all");
        cell.classList.add("meta");
        cell.style.textAlign = "right";
        cell.oncontextmenu = moveToTop;
        significantDigitsHtml(row.count).setInnerHtml(cell);
        cell.title = "Number of rows that have these values: " + formatNumber(row.count);

        // Maps a column name to a boolean indicating whether the
        // value is the same as in the previous row.  Must be computed
        // in sorted order.
        const isSame = new Map<string, boolean>();
        let previousSame = true;
        for (const o of this.order.sortOrientationList) {
            const name = o.columnDescription.name;
            if (!previousSame || previousRow == null) {
                isSame.set(name, false);
            } else {
                const index = this.order.find(name);
                if (previousRow.values[index] === row.values[index]) {
                    isSame.set(name, true);
                } else {
                    previousSame = false;
                    isSame.set(name, false);
                }
            }
        }

        for (let i = 0; i < cds.length; i++) {
            const cd = cds[i];
            const dataIndex = this.order.find(cd.name);
            let value: any;
            let borders: string;

            if (this.isVisible(cd.name)) {
                value = row.values[dataIndex];
                if (previousRow == null) {
                    if (last)
                        borders = "all";
                    else
                        borders = "top";
                } else if (last) {
                    if (isSame.get(cd.name))
                        borders = "bottom";
                    else
                        borders = "all";
                } else {
                    if (isSame.get(cd.name))
                        borders = "middle";
                    else
                        borders = "top";
                }
            } else {
                if (previousRow == null) {
                    if (last)
                        borders = "all";
                    else
                        borders = "top";
                } else {
                    if (last)
                        borders = "bottom";
                    else
                        borders = "middle";
                }
            }

            cell = this.grid.newCell(borders);
            let align = "right";
            if (kindIsString(cd.kind))
                align = "left";
            cell.style.textAlign = align;

            this.cellsPerColumn.get(cd.name).push(cell);

            if (this.isVisible(cd.name)) {
                let shownValue: string;
                if (value == null) {
                    cell.appendChild(makeMissing());
                    shownValue = "missing";
                } else {
                    shownValue = convertToStringFormat(row.values[dataIndex], cd.kind);
                    const high = this.highlight(shownValue);
                    cell.appendChild(high);
                }

                const shortValue = truncate(shownValue, 30);
                cell.title = shownValue + "\nRight click will popup a menu.";
                cell.oncontextmenu = (e) => {
                    this.contextMenu.clear();
                    // This menu shows the value to the right, but the filter
                    // takes the value to the left, so we have to flip all
                    // comparison signs.
                    this.contextMenu.addItem({text: "Keep " + shortValue,
                        action: () => this.filterOnValue(cd, value, "=="),
                        help: "Keep only the rows that have this value in this column.",
                    }, true);
                    this.contextMenu.addItem({text: "Keep different from " + shortValue,
                        action: () => this.filterOnValue(cd, value, "!="),
                        help: "Keep only the rows that have a different value in this column.",
                    }, true);
                    this.contextMenu.addItem({text: "Keep all < " + shortValue,
                        action: () => this.filterOnValue(cd, value, ">"),
                        help: "Keep only the rows that have a a smaller value in this column.",
                    }, true);
                    this.contextMenu.addItem({text: "Keep all > " + shortValue,
                        action: () => this.filterOnValue(cd, value, "<"),
                        help: "Keep only the rows that have a larger value in this column.",
                    }, true);
                    this.contextMenu.addItem({text: "Keep all <= " + shortValue,
                        action: () => this.filterOnValue(cd, value, ">="),
                        help: "Keep only the rows that have a smaller or equal value in this column.",
                    }, true);
                    this.contextMenu.addItem({text: "Keep all >= " + shortValue,
                        action: () => this.filterOnValue(cd, value, "<="),
                        help: "Keep only the rows that have a larger or equal in this column.",
                    }, true);
                    this.contextMenu.addItem({text: "Move to top",
                        action: () => this.moveRowToTop(row),
                        help: "Move this row to the top of the view.",
                    }, true);
                    this.contextMenu.show(e);
                };
            } else {
                cell.classList.add("empty");
                cell.innerHTML = "&nbsp;";
                cell.oncontextmenu = moveToTop;
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
export class NextKReceiver extends Receiver<NextKList> {
    constructor(page: FullPage,
                protected table: TableView,
                operation: ICancellable<NextKList>,
                protected reverse: boolean,
                protected order: RecordOrder,
                protected result: FindResult) {
        super(page, operation, "Getting table info");
    }

    public onNext(value: PartialResult<NextKList>): void {
        super.onNext(value);
        this.table.updateView(value.data, this.reverse, this.order,
            this.result, this.elapsedMilliseconds());
    }
}

/**
 * Receives a Schema and displays the resulting table.
 */
export class SchemaReceiver extends OnCompleteReceiver<TableSummary> {
    /**
     * Create a schema receiver for a new table.
     * @param page            Page where result should be displayed.
     * @param operation       Operation that will bring the results.
     * @param remoteObject    Table object.
     * @param dataset         Dataset that this is a part of.
     * @param forceTableView  If true the resulting view is always a table.
     */
    constructor(page: FullPage, operation: ICancellable<TableSummary>,
                protected remoteObject: TableTargetAPI,
                protected dataset: DatasetView,
                protected forceTableView) {
        super(page, operation, "Get schema");
    }

    public run(summary: TableSummary): void {
        if (summary.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }

        const schemaClass = new SchemaClass(summary.schema);
        if (summary.schema.length > 20 && !this.forceTableView) {
            const dataView = new SchemaView(this.remoteObject.remoteObjectId, this.page,
                summary.rowCount, schemaClass, this.elapsedMilliseconds());
            this.page.setDataView(dataView);
        } else {
            const nk: NextKList = {
                rowsScanned: summary.rowCount,
                startPosition: 0,
                rows: [],
            };

            const order = new RecordOrder([]);
            const table = new TableView(
                this.remoteObject.remoteObjectId, summary.rowCount, schemaClass, this.page);
            this.page.setDataView(table);
            table.updateView(nk, false, order, null, this.elapsedMilliseconds());
        }
    }
}

/**
 * Receives a row which is the result of an approximate quantile request and
 * initiates a request to get the NextK rows after this one.
 */
class QuantileReceiver extends OnCompleteReceiver<any[]> {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable<any[]>,
                       protected order: RecordOrder) {
        super(page, operation, "Compute quantiles");
    }

    public run(firstRow: any[]): void {
        const rr = this.tv.createNextKRequest(this.order, firstRow, this.tv.tableRowsDesired);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order, null));
    }
}

/**
 * Receives the result of a PCA computation and initiates the request
 * to project the specified columns using the projection matrix.
 */
export class CorrelationMatrixReceiver extends BaseRenderer {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable<RemoteObjectId>,
                       protected order: RecordOrder,
                       private numComponents: number,
                       private projectionName: string) {
        super(page, operation, "Correlation matrix", tv.dataset);
    }

    public run(): void {
        super.run();
        const rr = this.tv.createProjectToEigenVectorsRequest(
                this.remoteObject, this.numComponents, this.projectionName);
        rr.chain(this.operation);
        rr.invoke(new PCATableReceiver(
            this.page, rr, "Data with PCA projection columns", "Reading", this.tv, this.order,
            this.numComponents, this.tv.tableRowsDesired));
    }
}

// Receives the ID of a table that contains additional eigen vector projection columns.
// Invokes a sketch to get the schema of this new table.
class PCATableReceiver extends BaseRenderer {
    constructor(page: FullPage, operation: ICancellable<RemoteObjectId>,
                protected title: string, progressInfo: string,
                protected tv: TSViewBase, protected order: RecordOrder,
                protected numComponents: number,
                protected tableRowsDesired: number) {
        super(page, operation, progressInfo, tv.dataset);
    }

    public run(): void {
        super.run();
        const rr = this.remoteObject.createGetSchemaRequest();
        rr.chain(this.operation);
        rr.invoke(new PCASchemaReceiver(this.page, rr, this.remoteObject, this.tv,
            this.title, this.order, this.numComponents, this.tableRowsDesired));
    }
}

// Receives the schema after a PCA computation; computes the additional columns
// and adds these to the previous view
class PCASchemaReceiver extends OnCompleteReceiver<TableSummary> {
    constructor(page: FullPage, operation: ICancellable<TableSummary>,
                protected remoteObject: TableTargetAPI,
                protected tv: TSViewBase,
                protected title: string,
                protected order: RecordOrder,
                protected numComponents: number,
                protected tableRowsDesired: number) {
        super(page, operation, "Get schema");
    }

    public run(summary: TableSummary): void {
        if (summary.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }

        const newCols: IColumnDescription[] = [];
        const o = this.order.clone();
        // we rely on the fact that the last numComponents columns are added by the PCA
        // computation.
        for (let i = 0; i < this.numComponents; i++) {
            const cd = summary.schema[summary.schema.length - this.numComponents + i];
            newCols.push(cd);
            o.addColumn({ columnDescription: cd, isAscending: true });
        }

        const schema = this.tv.schema.concat(newCols);
        const table = new TableView(
            this.remoteObject.remoteObjectId, this.tv.rowCount, schema, this.page);
        this.page.setDataView(table);
        const rr = table.createNextKRequest(o, null, this.tableRowsDesired);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, table, rr, false, o, null));
    }
}

/**
 * Receives the id of a remote table and
 * initiates a request to display the nextK rows from this table.
 */
export class TableOperationCompleted extends BaseRenderer {
    public constructor(page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       protected rowCount: number,
                       protected schema: SchemaClass,
                       protected order: RecordOrder,
                       protected tableRowsDesired: number) {
        super(page, operation, "Table operation", page.dataset);
    }

    public run(): void {
        super.run();
        const table = new TableView(
            this.remoteObject.remoteObjectId, this.rowCount, this.schema, this.page);
        this.page.setDataView(table);
        const rr = table.createNextKRequest(this.order, null, this.tableRowsDesired);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, table, rr, false, this.order, null));
    }
}

/**
 * Receives a result from a remote table and initiates a NextK sketch
 * if any result is found.
 */
export class FindReceiver extends OnCompleteReceiver<FindResult> {
    public constructor(page: FullPage,
                       operation: ICancellable<FindResult>,
                       protected tv: TableView,
                       protected order: RecordOrder) {
        super(page, operation, "Compute quantiles");
    }

    public run(result: FindResult): void {
        if (result.at === 0) {
            this.page.reportError("No other matches found.");
            return;
        }
        const rr = this.tv.createNextKRequest(this.order, result.firstMatchingRow, this.tv.tableRowsDesired);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order, result));
    }
}

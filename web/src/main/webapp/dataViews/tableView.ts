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
    AggregateDescription,
    AggregateKind,
    allAggregateKind,
    ColumnSortOrientation, CompareDatasetsInfo,
    Comparison,
    ComparisonFilterDescription,
    FindResult,
    IColumnDescription,
    JSFilterInfo,
    kindIsString,
    ExtractValueFromKeyMapInfo,
    NextKList,
    RecordOrder,
    RemoteObjectId,
    RowData,
    RowFilterDescription,
    Schema,
    StringFilterDescription,
    TableSummary, CreateIntervalColumnMapInfo, RowValue
} from "../javaBridge";
import {OnCompleteReceiver, Receiver} from "../rpc";
import {DisplayName, SchemaClass} from "../schemaClass";
import {BaseReceiver, OnNextK, TableTargetAPI} from "../modules";
import {DataRangeUI} from "../ui/dataRangeUI";
import {IDataView} from "../ui/dataview";
import {Dialog, FieldKind, saveAs} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {ContextMenu, MenuItem, SubMenu, TopMenu, TopMenuItem} from "../ui/menu";
import {IScrollTarget, ScrollBar} from "../ui/scroll";
import {SelectionStateMachine} from "../ui/selectionStateMachine";
import {Resolution, SpecialChars, ViewKind} from "../ui/ui";
import {
    add,
    cloneToSet,
    Converters,
    find,
    formatNumber,
    ICancellable,
    makeMissing,
    makeSpan,
    PartialResult,
    sameAggregate,
    significantDigits,
    significantDigitsHtml,
    truncate,
    all, percent
} from "../util";
import {SchemaView} from "../modules";
import {SpectrumReceiver} from "./spectrumView";
import {TSViewBase} from "./tsViewBase";
import {Grid} from "../ui/grid";
import {LogFileReceiver} from "./logFileView";
import {FindBar} from "../ui/findBar";
import {HillviewToplevel} from "../toplevel";

/**
 * Displays a table in the browser.
 */
export class TableView extends TSViewBase implements IScrollTarget, OnNextK {
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
    protected strFilter: StringFilterDescription;
    public aggregates: AggregateDescription[] | null;

    // The following elements are used for Find
    protected findBar: FindBar;

    public constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage) {
        super(remoteObjectId, rowCount, schema, page, "Table");
        this.defaultProvenance = "Table";
        this.selectedColumns = new SelectionStateMachine();
        this.tableRowsDesired = Resolution.tableRowsOnScreen;
        this.order = new RecordOrder([]);
        this.topLevel = document.createElement("div");
        this.topLevel.id = "tableContainer";
        this.topLevel.tabIndex = 1;  // necessary for keyboard events?
        this.topLevel.onkeydown = (e) => this.keyDown(e);
        this.strFilter = null;
        this.aggregates = null;

        const items: TopMenuItem[] = [];
        items.push({
            text: "Export",
            help: "Save information from this view in a local file.",
            subMenu: new SubMenu([{
                text: "Schema",
                help: "Saves the schema of this data JSON file.",
                action: () => this.exportSchema()
            }, {
                text: "As CSV",
                help: "Saves the data in this view in a CSV file.",
                action: () => this.export()
            }]),
        });
        if (HillviewToplevel.instance.uiconfig.enableSaveAs)
            items.push(this.saveAsMenu());
        const viewSubMenu: MenuItem[] = [
            {
                text: "Refresh",
                action: () => this.refresh(),
                help: "Redraw this view.",
            }, {
                text: "No columns",
                action: () => this.setOrder(new RecordOrder([]), false),
                help: "Make all columns invisible",
            }, {
                text: "Schema",
                action: () => this.viewSchema(),
                help: "Browse the list of columns of this table and choose a subset to visualize.",
            }, {
                text: "Change table size...",
                action: () => this.changeTableSize(),
                help: "Change the number of rows displayed",
            }];
        items.push({
                text: "View", help: "Change the way the data is displayed.",
                subMenu: new SubMenu(viewSubMenu),
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
                            this.findBar.show(true);
                        }
                    },
                    {
                        text: "Filter...",
                        help: "Filter rows that contain a specific value",
                        action: () => this.showFilterDialog(
                            null, this.order, this.tableRowsDesired, this.aggregates) },
                    {
                        text: "Compare...",
                        help: "Filter rows by comparing with a specific value",
                        action: () => this.showCompareDialog(
                            null, this.order, this.tableRowsDesired, this.aggregates) },
                    {
                        text: "Filter using JavaScript...",
                        help: "Filter rows with JavaScript",
                        action: () => this.filterJSDialog() },
                    {
                        text: "Compare subsets...",
                        help: "Given other data views creates a column that indicates for each row the views it belongs to.",
                        action: () => this.setCompareDialog() },
                ]),
            },
            this.dataset.combineMenu(this, page.pageId));

        const menu = new TopMenu(items);
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
        this.findBar = new FindBar((n, f) => this.find(n, f), () => this.findFilter());
        this.topLevel.appendChild(this.findBar.getHTMLRepresentation());

        if (this.isPrivate()) {
            menu.enable("Filter", false);
            menu.enable("Combine", false);
            menu.getSubmenu("View").enable("Change table size...", false);
            const chart = menu.getSubmenu("Chart");
            chart.enable("2D Histogram...", false);
            chart.enable("Trellis 2D histograms...", false);
            chart.enable("Trellis heatmaps...", false);
        }

        this.createDiv("summary");
    }

    public export(): void {
        let lines = [];
        let line = "count";
        for (const o of this.order.sortOrientationList)
            line += "," + JSON.stringify(this.schema.displayName(o.columnDescription.name).displayName);
        if (this.aggregates != null)
            for (const a of this.aggregates) {
                // noinspection UnnecessaryLocalVariableJS
                const dn = this.schema.displayName(a.cd.name).displayName;
                line += "," + JSON.stringify(a.agkind + "(" + dn + "))");
            }
        lines.push(line);

        for (let i = 0; i < this.nextKList.rows.length; i++) {
            const row = this.nextKList.rows[i];
            line = row.count.toString();
            for (let j = 0; j < row.values.length; j++) {
                const kind = this.order.sortOrientationList[j].columnDescription.kind;
                let a = Converters.valueToString(row.values[j], kind);
                if (kindIsString(kind))
                    a = JSON.stringify(a);
                line += "," + a;
            }
            if (this.nextKList.aggregates != null) {
                const agg = this.nextKList.aggregates[i];
                for (const v of agg) {
                    line += "," + v;
                }
            }
            lines.push(line);
        }
        const fileName = "table.csv";
        saveAs(fileName, lines.join("\n"));
    }

    /**
     * This function is invoked when someone clicks the "Filter" button on the find bar.
     * This filters and keeps only rows that match the find criteria.
     */
    private findFilter(): void {
        const filter = this.findBar.getFilter();
        const columns = this.order.getSchema().map((c) => c.name);
        if (columns.length === 0) {
            this.page.reportError("No columns are visible");
            return;
        }
        const rr = this.createFilterColumnsRequest(
            { colNames: columns, stringFilterDescription: filter });
        const title = "Filtered on: " + filter.compareValue;
        const newPage = this.dataset.newPage(new PageTitle(title,
            Converters.stringFilterDescription(filter)), this.page);
        rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema,
            this.order, this.tableRowsDesired, this.aggregates));
    }

    public serialize(): IViewSerialization {
        // noinspection UnnecessaryLocalVariableJS
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
        const firstRow: RowValue[] = ser.firstRow;
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
            aggregates: null,
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

        const newFilter = this.findBar.getFilter();
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
        const topRow: RowValue[] = (fromTop ? null : this.nextKList.rows[0].values);
        const rr = this.createFindRequest(o, topRow, this.strFilter, excludeTopRow, next);
        rr.invoke(new FindReceiver(this.getPage(), rr, this, o));
    }

    // noinspection JSUnusedLocalSymbols
    protected getCombineRenderer(title: PageTitle):
        (page: FullPage, operation: ICancellable<RemoteObjectId>) => BaseReceiver {
        return (page: FullPage, operation: ICancellable<RemoteObjectId>) => {
            return new TableOperationCompleted(page, operation, this.rowCount, this.schema,
                this.order.clone(), this.tableRowsDesired, this.aggregates);
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
        const rr = this.createNextKRequest(order, this.nextKList.rows[0].values,
            this.tableRowsDesired, this.aggregates);
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
        const rr = this.createNextKRequest(o, null, this.tableRowsDesired, this.aggregates);
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
        const rr = this.createNextKRequest(order, null, this.tableRowsDesired, this.aggregates);
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
            o, this.nextKList.rows[this.nextKList.rows.length - 1].values,
            this.tableRowsDesired, this.aggregates);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    protected setOrder(o: RecordOrder, preserveFirstRow: boolean): void {
        let firstRow: RowValue[] = null;
        let minValues: string[] = null;
        if (preserveFirstRow &&
            this.nextKList != null &&
            this.nextKList.rows != null &&
            this.nextKList.rows.length > 0) {
            firstRow = [];
            minValues = [];
        }

        for (const cso of o.sortOrientationList) {
            const index = this.order.find(cso.columnDescription.name);
            if (firstRow != null) {
                if (index >= 0) {
                    firstRow.push(this.nextKList.rows[0].values[index]);
                } else {
                    firstRow.push(null);
                    // noinspection JSObjectNullOrUndefined
                    minValues.push(cso.columnDescription.name);
                }
            }
        }
        const rr = this.createNextKRequest(o, firstRow, this.tableRowsDesired,
            this.aggregates, minValues);
        rr.invoke(new NextKReceiver(this.getPage(), this, rr, false, o, null));
    }

    private getSortOrder(column: string): [boolean, number] {
        for (let i = 0; i < this.order.length(); i++) {
            const o = this.order.get(i);
            if (o.columnDescription.name === column)
                return [o.isAscending, i];
        }
        return null;
    }

    private isVisible(column: string): boolean {
        const so = this.getSortOrder(column);
        return so != null;
    }

    private isAscending(column: string): boolean {
        const so = this.getSortOrder(column);
        if (so == null) return null;
        return so[0];
    }

    private getSortIndex(column: string): number {
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
                          displayName: DisplayName,
                          help: string,
                          width: number,
                          isVisible: boolean,
                          isSortable: boolean): HTMLElement {
        const className = isSortable ? null : "meta";
        const th = this.grid.addHeader(width, cd.name, !isVisible, className);
        th.classList.add("preventselection");
        th.title = help;
        if (!isVisible) {
            th.style.fontWeight = "normal";
        } else if (isSortable) {
            const span = makeSpan("", false);
            span.innerHTML = this.getSortIndex(cd.name) + this.getSortArrow(cd.name); // #nosec
            span.style.cursor = "pointer";
            span.onclick = () => this.toggleOrder(cd.name);
            th.appendChild(span);
        }
        th.appendChild(makeSpan(displayName.toString(), false));
        return th;
    }

    protected toggleOrder(colName: string): void {
        const o = this.order.toggle(colName);
        this.setOrder(o, true);
    }

    public showColumns(order: number, first: boolean): void {
        // order is 0 to hide
        //         -1 to sort descending
        //          1 to sort ascending
        const o = this.order.clone();
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
        // If we are inserting the first column then we do not preserve the first row
        this.setOrder(o, !first);
    }

    public resize(): void {
        this.updateView(this.nextKList, false, this.order, null);
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
        const rr = this.createNextKRequest(this.order, firstRow,
            this.tableRowsDesired, this.aggregates);
        rr.invoke(new NextKReceiver(this.page, this, rr, false, this.order, null));
    }

    private createContextMenu(
        thd: HTMLElement, colIndex: number, visible: boolean): void {
        const cd = this.schema.get(colIndex);
        thd.oncontextmenu = (e) => {
            this.columnClick(colIndex, e);
            if (e.ctrlKey && (e.button === 1)) {
                // Ctrl + click is interpreted as a right-click on macOS.
                // This makes sure it's interpreted as a column click with Ctrl.
                return;
            }

            const selectedCount = this.selectedColumns.size();
            this.contextMenu.clear();
            if (visible) {
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
                }, !this.isPrivate());
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
                help: "Sort the data first on this column, in increasing order.",
            }, !this.isPrivate());
            this.contextMenu.addItem({
                text: "Sort descending",
                action: () => this.showColumns(-1, true),
                help: "Sort the data first on this column, in decreasing order",
            }, !this.isPrivate());
            const chartMenuIdx = this.contextMenu.addExpandableItem(
              {
                text: "Charts",
                action: () => null,
                help: "List of available charts to draw. " + "",
              }
            );
            this.contextMenu.addItem({
                text: "Rename...",
                action: () => this.renameColumn(),
                help: "Give a new name to this column.",
            }, selectedCount === 1);
            this.contextMenu.addItem({
                text: "Frequent Elements...",
                action: () => this.heavyHittersDialog(),
                help: "Find the values that occur most frequently in the selected columns.",
            }, !this.isPrivate());
            if (selectedCount > 1 &&
                all(this.getSelectedColNames(), b => this.isNumericColumn(b))) {
                this.contextMenu.addItem({
                    text: "Correlation",
                    action: () => this.correlate(),
                    help: "Compute pairwise corellation between a set of numeric columns"
                }, true);
                this.contextMenu.addItem({
                    text: "PCA...",
                    action: () => this.pca(true),
                    help: "Perform Principal Component Analysis on a set of numeric columns. " +
                        "This produces a smaller set of columns that preserve interesting properties of the data.",
                }, !this.isPrivate());
                this.contextMenu.addItem({
                    text: "Plot Singular Value Spectrum",
                    action: () => this.spectrum(true),
                    help: "Plot singular values for the selected columns. ",
                }, !this.isPrivate());
            }
            this.contextMenu.addItem({
                text: "Filter...",
                action: () => {
                    const colName = this.getSelectedColNames()[0];
                    const colDesc = this.schema.displayName(colName);
                    this.showFilterDialog(
                        colDesc, this.order, this.tableRowsDesired, this.aggregates);
                },
                help: "Eliminate data that matches/does not match a specific value.",
            }, selectedCount === 1 && !this.isPrivate());
            this.contextMenu.addItem({
                text: "Compare...",
                action: () => {
                    const colName = this.getSelectedColNames()[0];
                    this.showCompareDialog(this.schema.displayName(colName),
                        this.order, this.tableRowsDesired, this.aggregates);
                },
                help: "Eliminate data that matches/does not match a specific value.",
            }, selectedCount === 1 && !this.isPrivate());
            this.contextMenu.addItem({
                text: "Convert...",
                action: () => this.convert(this.schema.displayName(cd.name),
                    this.order, this.tableRowsDesired, this.aggregates),
                help: "Convert the data in the selected column to a different data type.",
            }, selectedCount === 1 && !this.isPrivate());
            this.contextMenu.addItem({
                text: "Create interval column...",
                action: () => this.createIntervalColumn(this.getSelectedColNames()),
                help: "Combine two numeric columns into a colum of intervals.",
            }, this.getSelectedColCount() == 2 &&
                all(this.getSelectedColNames(), c => this.isNumericColumn(c)) && !this.isPrivate());
            this.contextMenu.addItem({
                text: "Create column in JS...",
                action: () => this.createJSColumnDialog(
                    this.order, this.tableRowsDesired, this.aggregates),
                help: "Add a new column computed using Javascript from the selected columns.",
            }, !this.isPrivate());
            this.contextMenu.addItem({
                text: "Aggregate...",
                action: () => this.aggregateDialog(),
                help: "Compute aggregations on some columns"
            }, all(this.getSelectedColNames(), (b) => this.isNumericColumn(b)) && !this.isPrivate());
            if (selectedCount === 1 && this.isKVColumn(this.getSelectedColNames()[0]))
                    this.contextMenu.addItem({
                        text: "Extract value...",
                        action: () => {
                            const colName = this.getSelectedColNames()[0];
                            this.createKVColumnDialog(colName, this.tableRowsDesired);
                        },
                        help: "Extract a value associated with a specific key."
                    }, !this.isPrivate());
            this.contextMenu.insertSubMenu( chartMenuIdx, {
                text: "Histogram",
                action: () =>
                  this.chart(
                    this.schema.getDescriptions(this.getSelectedColNames()),
                    this.getSelectedColCount() === 1
                      ? "Histogram"
                      : "2DHistogram"
                  ),
                help:
                  "Plot the data in the selected columns as a histogram. " +
                  "Applies to one or two columns only.",
              },
              selectedCount >= 1 && selectedCount <= 2, false
            );
            this.contextMenu.insertSubMenu( chartMenuIdx, {
                text: "Quartile vector",
                action: () =>
                  this.chart(
                    this.schema.getDescriptions(this.getSelectedColNames()),
                    "QuartileVector"
                  ),
                help:
                  "Plot the data in the selected columns as a vector of quartiles. " +
                  "Applies to one or two columns only.",
              },
              selectedCount == 2, false
            );
            this.contextMenu.insertSubMenu( chartMenuIdx, {
                text: "Heatmap",
                action: () =>
                  this.chart(
                    this.schema.getDescriptions(this.getSelectedColNames()),
                    "Heatmap"
                  ),
                help:
                  "Plot the data in the selected columns as a heatmap. " +
                  "Applies to two columns only.",
              },
              selectedCount === 2, false
            );
            this.contextMenu.insertSubMenu( chartMenuIdx, {
                text: "Trellis histograms",
                action: () =>
                  this.chart(
                    this.schema.getDescriptions(this.getSelectedColNames()),
                    selectedCount > 2
                      ? "Trellis2DHistogram"
                      : "TrellisHistogram"
                  ),
                help:
                  "Plot the data in the selected columns as a Trellis plot of histograms. " +
                  "Applies to two or three columns only.",
              },
              selectedCount >= 2 && selectedCount <= 3, false
            );
            this.contextMenu.insertSubMenu( chartMenuIdx, {
                text: "Trellis heatmaps",
                action: () =>
                  this.chart(
                    this.schema.getDescriptions(this.getSelectedColNames()),
                    "TrellisHeatmap"
                  ),
                help:
                  "Plot the data in the selected columns as a Trellis plot of heatmaps. " +
                  "Applies to three columns only.",
              },
              selectedCount === 3 && !this.isPrivate(), true
            );
            this.contextMenu.show(e);
        };
    }

    public createIntervalColumn(cols: string[]): void {
        if (cols.length != 2) {
            this.page.reportError("Only 2 columns expected");
            return;
        }
        const dialog = new Dialog(
            "Create column of intervals",
            "Creates a column of intervals from two numeric columns.");
        const resultColumn = this.schema.uniqueColumnName(cols[0] + ":" + cols[1]);
        dialog.addTextField("column", "Column", FieldKind.String, resultColumn, "Column to create");
        dialog.addBooleanField("keep", "Keep original columns", false,
            "If selected the original columns are not removed");
        dialog.setAction(() => {
            const col = dialog.getFieldValue("column");
            const cd: IColumnDescription = { kind: "Interval", name: col };
            const args: CreateIntervalColumnMapInfo = {
                startColName: cols[0],
                endColName: cols[1],
                columnIndex: -1,
                newColName: col
            };
            const rr = this.createIntervalRequest(args);
            let schema = this.schema.append(cd);
            let o = this.order.clone();
            o.addColumn({columnDescription: cd, isAscending: true});
            const keep = dialog.getBooleanValue("keep");
            if (!keep) {
                schema = schema.filter((c) => cols.indexOf(schema.displayName(c.name).displayName) < 0);
                o.hide(cols[0]);
                o.hide(cols[1]);
            }
            const rec = new TableOperationCompleted(
                this.page, rr, this.rowCount, schema, o,
                this.tableRowsDesired, this.aggregates);
            rr.invoke(rec);
        })
        dialog.show();
    }

    public setCompareDialog(): void {
        const dialog = new Dialog(
            "Compare data from multiple views",
            "Select two other views; this will create a column that indicates for each row the views it belongs to");
        const resultColumn = this.schema.uniqueColumnName("Compare");
        dialog.addTextField("column", "Column", FieldKind.String, resultColumn, "Column to create");
        const pages = this.dataset.allPages
            .filter(p => p.dataView.getRemoteObjectId() != null);
        if (pages.length < 2) {
            this.page.reportError("Not enough views to compare");
            return;
        }
        const label = (p: FullPage) => p.pageId + ". " + p.title.getTextRepresentation(p) +
            "(" + p.title.provenance + ")";
        dialog.addSelectFieldAsObject("view0", "First view",
            pages, label, "First view to compare");
        dialog.addSelectFieldAsObject("view1", "Second view",
            pages, label,"Second view to compare");

        dialog.setAction(() => {
            const page0 = dialog.getFieldValueAsObject<FullPage>("view0");
            const page1 = dialog.getFieldValueAsObject<FullPage>("view1");
            if (page0 == null || page1 == null)
                return;

            const newColumn = dialog.getFieldValue("column");
            const args: CompareDatasetsInfo = {
                names: [page0.pageId.toString(), page1.pageId.toString()],
                otherIds: [page0.dataView.getRemoteObjectId(), page1.dataView.getRemoteObjectId()],
                outputName: newColumn
            };
            const rr = this.createCompareDatasetsRequest(args);
            const cd: IColumnDescription = { name: newColumn, kind: "String" };
            const schema = this.schema.append(cd);
            const o = this.order.clone();
            o.addColumn({columnDescription: cd, isAscending: true});
            const rec = new TableOperationCompleted(
                this.page, rr, this.rowCount, schema, o,
                this.tableRowsDesired, this.aggregates);
            rr.invoke(rec);
        });
        dialog.show();
    }

    public filterJSDialog(): void {
        const dialog = new Dialog(
            "Filter using JavaScript", "Specify a JavaScript function which filters each row.");
        dialog.addMultiLineTextField("function", "Function",
            "function filter(row) {", "  return row['col'] != 0;", "}",
            "A JavaScript function that computes a Boolean value for each row." +
            "The function has a single argument 'row'.  The row is a JavaScript map that can be indexed with " +
            "a column name (a string) and which produces a value.  Rows where the function returns 'true' are keps.");
        dialog.setCacheTitle("FilterJSDialog");
        dialog.setAction(() => {
            const fun = "function filter(row) {" + dialog.getFieldValue("function") + "}";
            const selColumns = cloneToSet(this.getSelectedColNames());
            const subSchema = this.schema.filter((c) => selColumns.has(c.name));
            const arg: JSFilterInfo = {
                jsCode: fun,
                schema: subSchema.schema,
                renameMap: subSchema.getRenameVector(),
            };
            const rr = this.createJSFilterRequest(arg);
            const title = "Filtered using JS";
            const newPage = this.dataset.newPage(new PageTitle(title, "Filter using JavaScript code\n" + fun), this.page);
            const rec = new TableOperationCompleted(
                newPage, rr, this.rowCount, this.schema, this.order, this.tableRowsDesired, this.aggregates);
            rr.invoke(rec);
        });
        dialog.show();
    }

    public aggregateDialog(): void {
        const dialog = new Dialog("Aggregate", "Select columns to display in aggregate");
        dialog.addSelectField("aggregation", "Operation", allAggregateKind,
            null, "Choose aggregation operation to perform");
        const selected = this.getSelectedColNames();
        dialog.setAction(() => {
            const operation = dialog.getFieldValue("aggregation");
            if (this.aggregates == null)
                this.aggregates = [];
            for (const col of selected) {
                const agg: AggregateDescription = {
                    cd: this.schema.find(col),
                    agkind: operation as AggregateKind
                };
                if (find(agg, this.aggregates, sameAggregate) > 0)
                    continue;
                this.aggregates.push(agg);
            }
            this.refresh();
        });
        dialog.show();
    }

    public updateView(nextKList: NextKList,
                      revert: boolean,
                      order: RecordOrder,
                      result: FindResult): void {
        this.grid.prepareForUpdate();
        this.selectedColumns.clear();
        this.nextKList = nextKList;
        if (nextKList == null)
            return;
        this.dataRowsDisplayed = 0;
        this.startPosition = nextKList.startPosition;
        this.rowCount = nextKList.rowsScanned;
        this.order = order.clone();
        if (this.isPrivate())
            this.page.setEpsilon(null, null);

        if (revert) {
            let rowsDisplayed = 0;
            if (nextKList.rows != null) {
                nextKList.rows.reverse();
                rowsDisplayed = nextKList.rows.map((r) => r.count).reduce(add, 0);
            }
            if (nextKList.aggregates != null)
                nextKList.aggregates.reverse();
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
            let thd = this.addHeaderCell(posCd, new DisplayName(posCd.name),
                "Position within sorted order.", DataRangeUI.width, true, false);
            thd.oncontextmenu = () => {};

            thd = this.addHeaderCell(ctCd, new DisplayName(ctCd.name),
                "Number of occurrences.", 75, true, false);
            thd.oncontextmenu = () => {};
            if (this.schema == null)
                return;
        }

        for (let i = 0; i < this.schema.length; i++) {
            const cd = this.schema.get(i);
            cds.push(cd);

            const kindString = cd.kind;
            const name = this.schema.displayName(cd.name);
            let title = name + "\nType is " + kindString + "\n";
            if (this.isPrivate()) {
                const pm = this.dataset.privacySchema.quantization.quantization[cd.name];
                if (pm != null) {
                    const eps = this.dataset.getEpsilon([cd.name]);
                    title += "Epsilon=" + eps + "\n";
                    if (kindIsString(cd.kind)) {
                        title += "Range is [" + pm.leftBoundaries[0] + ", " + pm.globalMax + "]\n";
                        title += "Divided in " + pm.leftBoundaries.length + " intervals\n";
                    } else if (cd.kind === "Date") {
                        title += "Range is [" + Converters.dateFromDouble(pm.globalMin as number) +
                            ", " + Converters.dateFromDouble(pm.globalMax as number) + "]\n";
                        title += "Bucket size is "
                            + Converters.intervalFromDouble(pm.granularity) + "\n";
                    } else {
                        title += "Range is [" + formatNumber(pm.globalMin as number) + ", " +
                            formatNumber(pm.globalMax as number) + "]\n";
                        title += "Bucket size is " + pm.granularity + "\n";
                    }
                }
            }
            title += "Right mouse click opens a menu\n";
            const visible = this.order.find(cd.name) >= 0;
            const thd = this.addHeaderCell(cd, name, title, 0, visible, true);
            thd.classList.add("col" + i.toString());
            thd.onclick = (e) => this.columnClick(i, e);
            thd.ondblclick = (e) => {
                e.preventDefault();
                const o = this.order.clone();
                if (visible)
                    o.hide(cd.name);
                else
                    o.addColumn({ columnDescription: cd, isAscending: true });
                this.setOrder(o, true);
            };
            this.createContextMenu(thd, i, visible);
        }

        if (this.aggregates != null) {
            for (let i = 0; i < this.aggregates.length; i++) {
                const ag = this.aggregates[i];
                let name;
                const dn = this.schema.displayName(ag.cd.name);
                name = ag.agkind + "(" + dn.toString() + ")";
                const cd: IColumnDescription = {
                    kind: ag.cd.kind,
                    name: name
                };
                const thd = this.addHeaderCell(cd, new DisplayName(name),
                    ag.agkind + " of values in column " + this.schema.displayName(ag.cd.name),
                    0, true, false);
                const aggIndex = i;
                thd.oncontextmenu = (e: MouseEvent) => {
                    this.contextMenu.clear();
                    this.contextMenu.addItem({
                        text: "Remove",
                        action: () => {
                            this.aggregates.splice(aggIndex, 1);
                            if (this.aggregates.length === 0)
                                this.aggregates = null;
                            this.refresh();
                        },
                        help: "Remove aggregate from display.",
                    }, true);
                    this.contextMenu.show(e);
                };
            }
        }

        this.cellsPerColumn = new Map<string, HTMLElement[]>();
        cds.forEach((cd) => this.cellsPerColumn.set(cd.name, []));
        let tableRowCount = 0;
        // Add row data
        let previousRow: RowData = null;
        if (nextKList.rows != null) {
            tableRowCount = nextKList.rows.length;
            let index = 0;
            if (nextKList.aggregates != null)
                console.assert(nextKList.rows.length === nextKList.aggregates.length);
            for (let i = 0; i < nextKList.rows.length; i++) {
                const row = nextKList.rows[i];
                const agg = nextKList.aggregates == null ? null : nextKList.aggregates[i];
                this.addRow(row, previousRow, agg, cds, index === nextKList.rows.length - 1);
                previousRow = row;
                index++;
            }
        }

        this.summary.set("table rows", tableRowCount);
        this.summary.set("displayed rows", this.dataRowsDisplayed, this.isPrivate());
        this.standardSummary();
        this.summary.set("% visible", percent(this.dataRowsDisplayed / this.rowCount));
        if (this.startPosition > 0) {
            this.summary.set("starting at %", percent(this.startPosition / this.rowCount));
        }
        this.summary.display();

        if (result != null) {
            this.findBar.setCounts(result.before, result.after);
        } else {
            this.strFilter = null;
            this.findBar.show(false);
        }

        this.updateScrollBar();
        this.highlightSelectedColumns();
        this.grid.updateCompleted();
    }

    /**
     * Return true if this column is a column in a log file that has a contents
     * that is of the form of a key=value.
     */
    private isKVColumn(col: string): boolean {
        const cd = this.schema.find(col);
        return (cd.kind === "Json" ||
            // This is a heuristic; this is tied to RFC5424 logs right now
            (this.dataset.isLog() && col === "StructuredData"));
    }

    public filterOnRowValue(row: RowValue[], comparison: Comparison): void {
        const filter: RowFilterDescription = {
            order: this.order,
            data: row,
            comparison: comparison
        };
        const rr = this.createRowFilterRequest(filter);
        const title = "Filtered: " + filter.comparison + " to row.";
        const newPage = this.dataset.newPage(new PageTitle(title, Converters.rowFilterDescription(filter)), this.page);
        rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema,
            this.order, this.tableRowsDesired, this.aggregates));
    }

    public filterOnValue(cd: IColumnDescription, value: string | number | number[], comparison: Comparison): void {
        let stringValue = null;
        let doubleValue = null;
        let intervalEnd = null;
        switch (cd.kind) {
            case "Json":
            case "String":
                stringValue = value as string;
                break;
            case "Integer":
            case "Double":
            case "Date":
            case "Duration":
                doubleValue = value as number;
                break;
            case "Interval":
                const a = value as number[];
                doubleValue = a[0];
                intervalEnd = a[1];
                break;
        }
        const cfd: ComparisonFilterDescription = {
            column: cd,
            stringValue,
            doubleValue,
            intervalEnd,
            comparison,
        };
        this.runComparisonFilter(cfd, this.order, this.tableRowsDesired, this.aggregates);
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
        const rr = this.createProjectRequest(schema.schema);
        // Remove all aggregates that depend on these columns
        let aggregates = [];
        if (this.aggregates != null) {
            for (const a of this.aggregates) {
                if (!selected.has(a.cd.name))
                    aggregates.push(a);
            }
        }
        if (aggregates.length === 0)
            aggregates = null;

        const rec = new TableOperationCompleted(this.page, rr, this.rowCount,
            schema, order, this.tableRowsDesired, aggregates);
        rr.invoke(rec);
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

    public correlate(): void {
        const colNames = this.getSelectedColNames();
        const [valid, message] = this.checkNumericColumns(colNames, 2);
        if (!valid) {
            this.page.reportError("Not valid for correlation:" + message);
            return;
        }
        this.chart(this.schema.getDescriptions(colNames), "CorrelationHeatmaps");
    }

    public pca(toSample: boolean): void {
        const colNames = this.getSelectedColNames();
        const [valid, message] = this.checkNumericColumns(colNames, 2);
        if (!valid) {
            this.page.reportError("Not valid for PCA:" + message);
            return;
        }

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
        const maxRows = 1000;
        const field = dialog.addTextField("rows", "Rows", FieldKind.Integer,
            Resolution.tableRowsOnScreen.toString(),
            "Number of rows to show (between 10 and " + maxRows.toString() + ")");
        field.min = "10";
        field.max = maxRows.toString();
        field.required = true;
        dialog.setAction(() => {
            const rowCount = dialog.getFieldValueAsInt("rows");
            if (rowCount < 10 || rowCount > maxRows) {
                this.page.reportError("Row count must be between 10 and " + maxRows.toString());
                return;
            }
            this.tableRowsDesired = rowCount;
            this.refresh();
        });
        dialog.show();
    }

    public viewSchema(): void {
        const newPage = this.dataset.newPage(new PageTitle("Schema", this.defaultProvenance), this.page);
        const sv = new SchemaView(this.remoteObjectId, newPage, this.rowCount, this.schema);
        newPage.setDataView(sv);
        sv.show(0);
        newPage.scrollIntoView();
    }

    public moveRowToTop(row: RowData): void {
        const rr = this.createNextKRequest(
            this.order, row.values, this.tableRowsDesired, this.aggregates);
        rr.invoke(new NextKReceiver(this.page, this, rr, false, this.order, null));
    }

    public addRow(row: RowData, previousRow: RowData | null,
                  agg: number[] | null,
                  cds: IColumnDescription[], last: boolean): void {
        this.grid.newRow();
        const position = this.startPosition + this.dataRowsDisplayed;
        const rowContextMenu = (e: MouseEvent) => {
            this.contextMenu.clear();
            this.contextMenu.addItem({text: "Keep equal rows",
                action: () => this.filterOnRowValue(row.values, "=="),
                help: "Keep only the rows that are equal to this one."
            }, true);
            this.contextMenu.addItem({text: "Keep different rows",
                action: () => this.filterOnRowValue(row.values, "!="),
                help: "Keep only the rows that are different."
            }, true);
            this.contextMenu.addItem({text: "Keep rows before",
                action: () => this.filterOnRowValue(row.values, ">"),
                help: "Keep only the rows that come before this one in the sort order."
            }, true);
            this.contextMenu.addItem({text: "Keep rows after",
                action: () => this.filterOnRowValue(row.values, "<"),
                help: "Keep only the rows that come after this one in the sort order."
            }, true);
            this.contextMenu.addItem({text: "Keep all rows before and this one",
                action: () => this.filterOnRowValue(row.values, ">="),
                help: "Keep only this row and the rows that come before this one in the sort order."
            }, true);
            this.contextMenu.addItem({text: "Keep all rows after and this one",
                action: () => this.filterOnRowValue(row.values, "<="),
                help: "Keep only this rows and the rows that cone after this one in the sort order."
            }, true);
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
        cell.oncontextmenu = rowContextMenu;

        cell = this.grid.newCell("all");
        cell.classList.add("meta");
        cell.style.textAlign = "right";
        cell.oncontextmenu = rowContextMenu;
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
            let value: RowValue;
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
                    shownValue = Converters.valueToString(row.values[dataIndex], cd.kind);
                    const high = this.findBar.highlight(shownValue, this.strFilter);
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
                        help: "Keep only the rows that have this value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Keep different from " + shortValue,
                        action: () => this.filterOnValue(cd, value, "!="),
                        help: "Keep only the rows that have a different value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Keep all < " + shortValue,
                        action: () => this.filterOnValue(cd, value, ">"),
                        help: "Keep only the rows that have a smaller value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Keep all > " + shortValue,
                        action: () => this.filterOnValue(cd, value, "<"),
                        help: "Keep only the rows that have a larger value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Keep all <= " + shortValue,
                        action: () => this.filterOnValue(cd, value, ">="),
                        help: "Keep only the rows that have a smaller or equal value in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Keep all >= " + shortValue,
                        action: () => this.filterOnValue(cd, value, "<="),
                        help: "Keep only the rows that have a larger or equal in this column."
                    }, true);
                    this.contextMenu.addItem({text: "Move to top",
                        action: () => this.moveRowToTop(row),
                        help: "Move this row to the top of the view."
                    }, true);
                    /*
                    if (this.dataset.isLog() &&
                        cd.name === "Filename" && row.count === 1) {
                        this.contextMenu.addItem({
                            text: "Open file",
                            action: () => this.openLogFile(row, value),
                            help: "Open this file in a new tab"
                        }, true);
                    }
                    */
                    this.contextMenu.show(e);
                };
            } else {
                cell.classList.add("empty");
                cell.innerHTML = "&nbsp;";
                cell.oncontextmenu = rowContextMenu;
            }
        }

        if (agg != null) {
            console.assert(agg.length === this.aggregates.length);
            for (let i = 0; i < agg.length; i++) {
                cell = this.grid.newCell("all");
                cell.style.textAlign = "right";
                const shownValue = significantDigits(agg[i]);
                const span = makeSpan(shownValue);
                cell.classList.add("meta");
                cell.appendChild(span);
                cell.oncontextmenu = rowContextMenu;
                cell.title = formatNumber(agg[i]);
            }
        }

        this.dataRowsDisplayed += row.count;
    }

    public openLogFile(row: RowData, filename: string): void {
        const rr = this.createContainsRequest(this.order, row.values);
        rr.invoke(new LogFileReceiver(this.page, rr, filename, this.schema,
            this.order.getSchema(), row.values));
    }

    public setScroll(top: number, bottom: number): void {
        this.scrollBar.setPosition(top, bottom);
    }

    public createKVColumnDialog(inputColumn: string, tableRowsDesired: number): void {
        const dialog = new Dialog(
            "Extract value", "Extract values associated with a specific key.");
        const kf = dialog.addTextField("key", "Key", FieldKind.String, null, "Key whose value is extracted.");
        kf.onchange = () => {
            dialog.setFieldValue("outColName",
                this.schema.uniqueColumnName(dialog.getFieldValue("key")));
        };
        const name = dialog.addTextField(
            "outColName", "Column name", FieldKind.String, null, "Name to use for the generated column.");
        name.required = true;
        dialog.setCacheTitle("CreateKVDialog");
        dialog.setAction(() => this.createKVColumn(dialog, inputColumn, tableRowsDesired));
        dialog.show();
    }

    private createKVColumn(dialog: Dialog, inputColumn: string, tableRowsDesired: number): void {
        const col = dialog.getFieldValue("outColName");
        if (this.schema.find(col) != null) {
            this.page.reportError("Column " + col + " already exists");
            return;
        }
        const key = dialog.getFieldValue("key");
        if (key === "") {
            this.page.reportError("Please specify a non-empty key");
            return;
        }
        const arg: ExtractValueFromKeyMapInfo = {
            key: key,
            inputColumn: this.schema.find(inputColumn),
            outputColumn: col,
            outputIndex: this.schema.columnIndex(inputColumn)
        };
        const rr = this.createKVCreateColumnRequest(arg);
        const cd: IColumnDescription = {
            kind: "String",
            name: col,
        };
        const schema = this.schema.append(cd);
        const o = this.order.clone();
        o.addColumn({columnDescription: cd, isAscending: true});

        const rec = new TableOperationCompleted(
            this.page, rr, this.rowCount, schema, o, tableRowsDesired, this.aggregates);
        rr.invoke(rec);
    }
}

/**
 * Receives the NextK rows from a table and displays them.
 */
export class NextKReceiver extends Receiver<NextKList> {
    constructor(page: FullPage,
                protected view: OnNextK,
                operation: ICancellable<NextKList>,
                protected reverse: boolean,
                protected order: RecordOrder,
                protected result: FindResult) {
        super(page, operation, "Getting table rows");
    }

    public onNext(value: PartialResult<NextKList>): void {
        super.onNext(value);
        this.view.updateView(value.data, this.reverse, this.order, this.result);
    }

    public onCompleted(): void {
        super.onCompleted();
        this.view.updateCompleted(this.elapsedMilliseconds());
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
     * @param schema          Schema that is used to display the data.
     * @param viewKind        What view to use.  If null we get to choose.
     */
    constructor(page: FullPage, operation: ICancellable<TableSummary>,
                protected remoteObject: TableTargetAPI,
                protected dataset: DatasetView,
                protected schema: SchemaClass,
                protected viewKind: ViewKind | null) {
        super(page, operation, "Get schema");
    }

    public run(ts: TableSummary): void {
        if (ts.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }
        if (ts.metadata != null)
            this.dataset.setPrivate(ts.metadata);
        const schemaClass = this.schema == null ? new SchemaClass(ts.schema) : this.schema;
        const useSchema = this.viewKind === "Schema" ||
            (this.viewKind === null && ts.schema.length > 20);
        if (useSchema) {
            const dataView = new SchemaView(this.remoteObject.remoteObjectId, this.page,
                ts.rowCount, schemaClass);
            this.page.setDataView(dataView);
            dataView.show(this.elapsedMilliseconds());
        } else {
            const nk: NextKList = {
                rowsScanned: ts.rowCount,
                startPosition: 0,
                rows: [],
                aggregates: null
            };

            const order = new RecordOrder([]);
            const table = new TableView(this.remoteObject.remoteObjectId, ts.rowCount,
                schemaClass, this.page);
            this.page.setDataView(table);
            table.updateView(nk, false, order, null);
            table.updateCompleted(this.elapsedMilliseconds());
        }
    }
}

/**
 * Receives a row which is the result of an approximate quantile request and
 * initiates a request to get the NextK rows after this one.
 */
class QuantileReceiver extends OnCompleteReceiver<RowValue[]> {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable<RowValue[]>,
                       protected order: RecordOrder) {
        super(page, operation, "Compute quantiles");
    }

    public run(firstRow: RowValue[]): void {
        const rr = this.tv.createNextKRequest(
            this.order, firstRow, this.tv.tableRowsDesired, this.tv.aggregates);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order, null));
    }
}

/**
 * Receives the result of a PCA computation and initiates the request
 * to project the specified columns using the projection matrix.
 */
export class CorrelationMatrixReceiver extends BaseReceiver {
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable<RemoteObjectId>,
                       protected order: RecordOrder,
                       private numComponents: number,
                       private projectionName: string) {
        super(page, operation, "Correlation matrix", tv.dataset);
    }

    public run(value: RemoteObjectId): void {
        super.run(value);
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
class PCATableReceiver extends BaseReceiver {
    constructor(page: FullPage, operation: ICancellable<RemoteObjectId>,
                protected title: string, progressInfo: string,
                protected tv: TSViewBase, protected order: RecordOrder,
                protected numComponents: number,
                protected tableRowsDesired: number) {
        super(page, operation, progressInfo, tv.dataset);
    }

    public run(value: RemoteObjectId): void {
        super.run(value);
        const rr = this.remoteObject.createGetSummaryRequest();
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

    public run(ts: TableSummary): void {
        if (ts.schema == null) {
            this.page.reportError("No schema received; empty dataset?");
            return;
        }

        const newCols: IColumnDescription[] = [];
        const o = this.order.clone();
        // we rely on the fact that the last numComponents columns are added by the PCA
        // computation.
        for (let i = 0; i < this.numComponents; i++) {
            const cd = ts.schema[ts.schema.length - this.numComponents + i];
            newCols.push(cd);
            o.addColumn({ columnDescription: cd, isAscending: true });
        }

        const schema = this.tv.schema.concat(newCols);
        const table = new TableView(
            this.remoteObject.remoteObjectId, this.tv.rowCount, schema, this.page);
        this.page.setDataView(table);
        const rr = table.createNextKRequest(o, null, this.tableRowsDesired, null);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, table, rr, false, o, null));
    }
}

/**
 * Receives the id of a remote table and initiates a request to display the table.
 */
export class TableOperationCompleted extends BaseReceiver {
    /**
     * @param page       Page where the new view will be displayed
     * @param operation  Operation that initiated this request.
     * @param rowCount   Number of rows in the table.
     * @param schema     Table schema.
     * @param order      Order desired for table rows.  If this is null we will
     *                   actually display a SchemaView, otherwise we will display a TableView
     * @param tableRowsDesired  Number of rows desired in table (for a table view).
     * @param aggregates Aggregations that are desired.
     */
    public constructor(page: FullPage,
                       operation: ICancellable<RemoteObjectId>,
                       protected rowCount: number,
                       protected schema: SchemaClass,
                       protected order: RecordOrder,
                       protected tableRowsDesired: number,
                       protected aggregates: AggregateDescription[] | null) {
        super(page, operation, "Table operation", page.dataset);
    }

    public run(value: RemoteObjectId): void {
        super.run(value);
        if (this.order == null) {
            const rr = this.remoteObject.createGetSummaryRequest();
            rr.chain(this.operation);
            rr.invoke(new SchemaReceiver(
                this.page, rr, this.remoteObject, this.page.dataset, this.schema, "Schema"));
        } else {
            const table = new TableView(
                this.remoteObject.remoteObjectId, this.rowCount, this.schema, this.page);
            table.aggregates = this.aggregates;
            this.page.setDataView(table);
            const rr = table.createNextKRequest(
                this.order, null, this.tableRowsDesired, this.aggregates);
            rr.chain(this.operation);
            rr.invoke(new NextKReceiver(this.page, table, rr, false, this.order, null));
        }
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
        super(page, operation, "Searching for data");
    }

    public run(result: FindResult): void {
        if (result.at === 0) {
            this.page.reportError("No other matches found.");
            return;
        }
        const rr = this.tv.createNextKRequest(
            this.order, result.firstMatchingRow, this.tv.tableRowsDesired, this.tv.aggregates);
        rr.chain(this.operation);
        rr.invoke(new NextKReceiver(this.page, this.tv, rr, false, this.order, result));
    }
}

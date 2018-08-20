/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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
    allContentsKind, asContentsKind, ColumnSortOrientation, ComparisonFilterDescription,
    CreateColumnInfo, EqualityFilterDescription, HLogLog, IColumnDescription,
    RecordOrder, RemoteObjectId,
} from "../javaBridge";
import {OnCompleteReceiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BigTableView} from "../tableTarget";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage} from "../ui/fullPage";
import {SubMenu, TopMenuItem} from "../ui/menu";
import {ChartKind, SpecialChars, ViewKind} from "../ui/ui";
import {
    cloneToSet, Comparison, Converters, ICancellable, mapToArray, significantDigits,
} from "../util";
import {HeavyHittersReceiver, HeavyHittersView} from "./heavyHittersView";
import {DataRangesCollector} from "./histogram2DView";
import {
    DataRangeCollector, HistogramDialog,
} from "./histogramView";
import {TableOperationCompleted, TableView} from "./tableView";
import {PlottingSurface} from "../ui/plottingSurface";
import {HistogramViewBase} from "./histogramViewBase";

/**
 * A base class for TableView and SchemaView.
 */
export abstract class TSViewBase extends BigTableView {
    protected constructor(
        remoteObjectId: RemoteObjectId,
        rowCount: number,
        schema: SchemaClass,
        page: FullPage,
        viewKind: ViewKind) {
        super(remoteObjectId, rowCount, schema, page, viewKind);
    }

    /**
     * Get the list of the column names for the columns that are selected.
     * These are columns in the underlying data table.
     */
    public abstract getSelectedColNames(): string[];

    /**
     * Get the number of columns that are selected.
     * These are columns in the underlying data table.
     */
    public abstract getSelectedColCount(): number;

    public renameColumn(): void {
        const cols = this.getSelectedColNames();
        if (cols.length !== 1) {
            this.reportError("Select only 1 column to rename");
            return;
        }
        const colName = cols[0];
        const dialog = new Dialog("Rename column",
            "Choose a new name for column " + this.schema.displayName(colName));
        dialog.addTextField("name", "New name",
            FieldKind.String, this.schema.displayName(colName), "New name to use for column");
        dialog.setAction(() => this.doRenameColumn(colName, dialog.getFieldValue("name")));
        dialog.show();
    }

    public doRenameColumn(from: string, to: string): void {
        this.schema = this.schema.clone();
        if (!this.schema.changeDisplayName(from, to)) {
            this.reportError("Cannot rename column to " + to + " since the name is already used.");
            return;
        }
        this.refresh();
    }

    public reportError(s: string) {
        this.page.reportError(s);
    }

    public saveAsOrc(schema: SchemaClass): void {
        const dialog = new Dialog("Save as ORC files",
            "Describe the set of ORC files where data will be saved.");
        dialog.addTextField("folderName", "Folder", FieldKind.String, "/",
            "All ORC files will be written to this folder on each of the remote machines.");
        dialog.setCacheTitle("saveAsDialog");

        class SaveReceiver extends OnCompleteReceiver<boolean> {
            constructor(page: FullPage, operation: ICancellable<boolean>) {
                super(page, operation, "Save as ORC files");
            }

            public run(value: boolean): void {
                if (value)
                    this.page.reportError("Save succeeded.");
            }
        }

        dialog.setAction(() => {
            const folder = dialog.getFieldValue("folderName");
            const rr = this.createStreamingRpcRequest<boolean>("saveAsOrc", {
                folder,
                schema: schema.schema,
                renameMap: mapToArray(schema.getRenameMap()),
            });
            const renderer = new SaveReceiver(this.page, rr);
            rr.invoke(renderer);
        });
        dialog.show();
    }

    public createColumnDialog(order: RecordOrder, tableRowsDesired: number): void {
        const dialog = new Dialog(
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
        dialog.setAction(() => this.createColumn(dialog, order, tableRowsDesired));
        dialog.show();
    }

    public createColumn(dialog: Dialog, order: RecordOrder, tableRowsDesired: number): void {
        const col = dialog.getFieldValue("outColName");
        const kind = dialog.getFieldValue("outColKind");
        const fun = "function map(row) {" + dialog.getFieldValue("function") + "}";
        const selColumns = cloneToSet(this.getSelectedColNames());
        const subSchema = this.schema.filter((c) => selColumns.has(c.name));
        const arg: CreateColumnInfo = {
            jsFunction: fun,
            outputColumn: col,
            outputKind: asContentsKind(kind),
            schema: subSchema.schema,
            renameMap: mapToArray(subSchema.getRenameMap()),
        };
        const rr = this.createCreateColumnRequest(arg);
        const newPage = this.dataset.newPage("New column " + col, this.page);
        const cd: IColumnDescription = {
            kind: arg.outputKind,
            name: col,
        };
        const schema = this.schema.append(cd);
        const o = order.clone();
        o.addColumn({columnDescription: cd, isAscending: true});

        const rec = new TableOperationCompleted(newPage, rr, this.rowCount, schema, o, tableRowsDesired);
        rr.invoke(rec);
    }

    protected histogram1D(cd: IColumnDescription): void {
        const size = PlottingSurface.getDefaultChartSize(this.page.getWidthInPixels());
        const rr = this.getDataRange(cd, size.width);
        rr.invoke(new DataRangeCollector(
            this, this.page, rr, this.schema, 0, cd, null, size.width,
            { exact: false, reusePage: false }));
    }

    protected histogram2D(cds: IColumnDescription[]): void {
        const buckets = HistogramViewBase.maxHistogram2DBuckets(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: false, relative: false,
            chartKind: ChartKind.Histogram, exact: false
        }));
    }

    protected trellis2D(cds: IColumnDescription[]): void {
        const buckets = HistogramViewBase.maxHistogram2DBuckets(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: false, relative: false,
            chartKind: ChartKind.TrellisHistogram, exact: false
        }));
    }

    protected trellis3D(cds: IColumnDescription[]): void {
        // TODO
    }

    protected histogram(columns: string[]): void {
        const cds = this.schema.getDescriptions(columns);
        if (cds.length === 1) {
            this.histogram1D(cds[0]);
        } else {
            this.histogram2D(cds);
        }
    }

    protected trellis(columns: string[]): void {
        const cds = this.schema.getDescriptions(columns);
        if (cds.length === 2) {
            this.trellis2D(cds);
        } else {
            this.trellis3D(cds);
        }
    }

    protected heatmap(columns: string[]) {
        const cds = this.schema.getDescriptions(columns);
        const buckets = HistogramViewBase.maxHeatmapBuckets(this.page);
        const rr = this.getDataRanges2D(cds, buckets);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema, 0, cds, null, {
            reusePage: false,
            relative: false,
            chartKind: ChartKind.Heatmap,
            exact: true
        }));
    }

    protected heatmapSelected(): void {
        if (this.getSelectedColCount() !== 2) {
            this.reportError("Must select 2 columns for heatmap");
            return;
        }
        this.heatmap(this.getSelectedColNames());
    }

    protected histogramSelected(): void {
        if (this.getSelectedColCount() < 1 || this.getSelectedColCount() > 2) {
            this.reportError("Must select 1 or 2 columns for histogram");
            return;
        }
        this.histogram(this.getSelectedColNames());
    }

    protected trellisSelected(): void {
        if (this.getSelectedColCount() < 2 || this.getSelectedColCount() > 3) {
            this.reportError("Must select 1 or 2 columns for Trellis polots");
            return;
        }
        this.trellis(this.getSelectedColNames());
    }

    public twoDHistogramMenu(heatmap: boolean): void {
        if (this.schema.length < 2) {
            this.reportError("Could not find two columns that can be charted.");
            return;
        }

        const allColumns = this.schema.columnNames.map((c) => this.schema.displayName(c));
        const label = heatmap ? "heatmap" : "2D histogram";
        const dia = new Dialog(label,
                        "Display a " + label + " of the data in two columns");
        dia.addSelectField("columnName0", "First column", allColumns, allColumns[0],
                    "First column (X axis)");
        dia.addSelectField("columnName1", "Second column", allColumns, allColumns[1],
                    "Second column " + (heatmap ? "(Y axis)" : "(color)"));
        dia.setAction(
            () => {
                const c0 = dia.getFieldValue("columnName0");
                const col0 = this.schema.fromDisplayName(c0);
                const c1 = dia.getFieldValue("columnName1");
                const col1 = this.schema.fromDisplayName(c1);
                if (col0 === col1) {
                    this.reportError("The two columns must be distinct");
                    return;
                }
                if (heatmap)
                    this.histogram([col0, col1]);
                else
                    this.heatmap([col0, col1]);
            },
        );
        dia.show();
    }

    public trellisMenu(twoD: boolean): void {
        const count = twoD ? 2 : 3;
        if (this.schema.length < count) {
            this.reportError("Could not find enough columns that can be charted.");
            return;
        }

        const heatmap = false;  // TODO
        const allColumns = this.schema.columnNames.map((c) => this.schema.displayName(c));
        const label = heatmap ? "heatmap" : "2D histogram";
        const dia = new Dialog(label,
            "Display a Trellis plot of " + label + "s");
        dia.addSelectField("columnName0", "First column", allColumns, allColumns[0],
            "First column (X axis)");
        dia.addSelectField("columnName1", "Second column", allColumns, allColumns[1],
            count === 2 ? "Column to group by" :
                "Second column " + (heatmap ? "(Y axis)" : "(color)"));
        if (count === 3)
            dia.addSelectField("columnName2", "Third column", allColumns, allColumns[2],
                "Column to group by");

        dia.setAction(
            () => {
                const c0 = dia.getFieldValue("columnName0");
                const col0 = this.schema.fromDisplayName(c0);
                const c1 = dia.getFieldValue("columnName1");
                const col1 = this.schema.fromDisplayName(c1);
                if (col0 === col1) {
                    this.reportError("The columns must be distinct");
                    return;
                }
                let col2 = null;
                const colNames = [col0, col1];

                if (count === 3) {
                    const c2 = dia.getFieldValue("columnName2");
                    col2 = this.schema.fromDisplayName(c2);
                    if (col0 === col2 || col1 === col2) {
                        this.reportError("The columns must be distinct");
                        return;
                    }
                    colNames.push(col2);
                }
                this.trellis(colNames);
            },
        );
        dia.show();
    }

    protected hLogLog(): void {
        if (this.getSelectedColCount() !== 1) {
            this.reportError("Only one column must be selected");
            return;
        }
        const colName = this.getSelectedColNames()[0];
        const rr = this.createHLogLogRequest(colName);
        const rec = new CountReceiver(this.getPage(), rr, this.schema.displayName(colName));
        rr.invoke(rec);
    }

    public oneDHistogramMenu(): void {
        const dia = new HistogramDialog(
            this.schema.columnNames.map((c) => this.schema.displayName(c)));
        dia.setAction(
            () => {
                const col = this.schema.fromDisplayName(dia.getColumn());
                this.histogram([col]);
            },
        );
        dia.show();
    }

    public saveAsMenu(): TopMenuItem {
        return {
            text: "Save as", help: "Save the data to persistent storage.", subMenu: new SubMenu([
                {
                    text: "Save as ORC files...",
                    action: () => this.saveAsOrc(this.schema),
                    help: "Save the data to a set of ORC files on the remote machines.",
                },
            ]),
        };
    }

    public chartMenu(): TopMenuItem {
        return {
            text: "Chart", help: "Draw a chart", subMenu: new SubMenu([
                { text: "1D Histogram...", action: () => this.oneDHistogramMenu(),
                    help: "Draw a histogram of the data in one column."},
                { text: "2D Histogram....", action: () => this.twoDHistogramMenu(false),
                    help: "Draw a histogram of the data in two columns."},
                { text: "Heatmap...", action: () => this.twoDHistogramMenu(true),
                    help: "Draw a heatmap of the data in two columns."},
                { text: "Trellis 2D...", action: () => this.trellisMenu(true),
                    help: "Draw a Trellism plot of the data in two columns."},
                { text: "Trellis 3D...", action: () => this.trellisMenu(false),
                    help: "Draw a Trellism plot of the data in three columns."},
            ]),
        };
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param colName     Column name.  If null the user will select the column.
     * @param order  Current record ordering.
     * @param tableRowsDesired  Number of table rows to display.
     */
    protected showFilterDialog(
        colName: string, order: RecordOrder, tableRowsDesired: number): void {
        const cd = this.schema.find(colName);
        const ef = new EqualityFilterDialog(cd, this.schema);
        ef.setAction(() => {
            const filter = ef.getFilter();
            const desc = this.schema.find(filter.column);
            const o = order.clone();
            const so: ColumnSortOrientation = {
                columnDescription: desc,
                isAscending: true,
            };
            o.addColumn(so);
            const rr = this.createFilterEqualityRequest(filter);
            const title = "Filtered: " + filter.column + " is " +
                (filter.complement ? "not " : "") +
                TableView.convert(filter.compareValue, desc.kind);

            const newPage = this.dataset.newPage(title, this.page);
            rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema, o, tableRowsDesired));
        });
        ef.show();
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param displayName  Column name.  If null the user will select the column.
     * @param order   Current record ordering.
     * @param tableRowsDesired  Number of table rows to display.
     */
    protected showCompareDialog(
        displayName: string, order: RecordOrder, tableRowsDesired: number): void {
        const cd = this.schema.findByDisplayName(displayName);
        const cfd = new ComparisonFilterDialog(cd, displayName, this.schema);
        cfd.setAction(() => this.runComparisonFilter(cfd.getFilter(), order, tableRowsDesired));
        cfd.show();
    }

    protected runComparisonFilter(
        filter: ComparisonFilterDescription, order: RecordOrder, tableRowsDesired: number): void {
        const cd = this.schema.find(filter.column);
        const kind = cd.kind;
        const so: ColumnSortOrientation = {
            columnDescription: cd, isAscending: true,
        };
        const o = order.clone();
        o.addColumn(so);

        const rr = this.createFilterComparisonRequest(filter);
        const title = "Filtered: " +
            TableView.convert(filter.compareValue, kind) + " " + filter.comparison + " " +
            this.schema.displayName(filter.column);

        const newPage = this.dataset.newPage(title, this.page);
        rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema, o, tableRowsDesired));
    }

    protected runHeavyHitters(percent: number) {
        if (percent == null || percent < HeavyHittersView.min || percent > 100) {
            this.reportError("Percentage must be between " + HeavyHittersView.min.toString() + " and 100");
            return;
        }
        const isApprox: boolean = true;
        const columnsShown: IColumnDescription[] = [];
        const cso: ColumnSortOrientation[] = [];
        this.getSelectedColNames().forEach((v) => {
            const colDesc = this.schema.find(v);
            columnsShown.push(colDesc);
            cso.push({columnDescription: colDesc, isAscending: true});
        });
        const order = new RecordOrder(cso);
        const rr = this.createHeavyHittersRequest(
            columnsShown, percent, this.getTotalRowCount(), HeavyHittersView.switchToMG);
        rr.invoke(new HeavyHittersReceiver(
            this.getPage(), this, rr, this.rowCount, this.schema,
            order, isApprox, percent, columnsShown, false));
    }

    protected heavyHittersDialog(): void {
        let title = "Frequent Elements from ";
        const cols: string[] = this.getSelectedColNames();
        if (cols.length <= 1) {
            title += " " + this.schema.displayName(cols[0]);
        } else {
            title += cols.length + " columns";
        }
        const d = new Dialog(title, "Find the most frequent values in the selected columns.");
        d.addTextField("percent", "Threshold (%)", FieldKind.Double, "1",
            "All values that appear in the dataset with a frequency above this value (as a percent) " +
            "will be considered frequent elements.  Must be a number between " + HeavyHittersView.minString +
            " and 100%.");
        d.setAction(() => {
            const amount = d.getFieldValueAsNumber("percent");
            if (amount != null)
                this.runHeavyHitters(amount);
        });
        d.setCacheTitle("HeavyHittersDialog");
        d.show();
    }

    public getTotalRowCount(): number {
        return this.rowCount;
    }
}

class EqualityFilterDialog extends Dialog {
    constructor(private columnDescription: IColumnDescription, private schema: SchemaClass) {
        super("Filter", "Eliminates data from a column according to its value.");
        if (columnDescription == null) {
            const cols = this.schema.columnNames.map((c) => this.schema.displayName(c));
            if (cols.length === 0)
                return;
            this.addSelectField("column", "Column", cols, null, "Column that is filtered");
        }
        this.addTextField("query", "Find", FieldKind.String, null, "Value to search");
        this.addBooleanField("asRegEx", "Interpret as Regular Expression", false, "Select "
            + "checkbox to interpret the search query as a regular expression");
        this.addBooleanField("complement", "Exclude matches", false, "Select checkbox to "
            + "filter out all matches");
        this.setCacheTitle("EqualityFilterDialog");
    }

    public getFilter(): EqualityFilterDescription {
        let textQuery: string = this.getFieldValue("query");
        if (this.columnDescription == null) {
            const colName = this.getFieldValue("column");
            this.columnDescription = this.schema.find(this.schema.fromDisplayName(colName));
        }
        if (this.columnDescription.kind === "Date") {
            const date = new Date(textQuery);
            textQuery = Converters.doubleFromDate(date).toString();
        }
        const asRegEx = this.getBooleanValue("asRegEx");
        const complement = this.getBooleanValue("complement");
        return {
            column: this.columnDescription.name,
            compareValue: textQuery,
            complement,
            asRegEx,
        };
    }
}

class ComparisonFilterDialog extends Dialog {
    private explanation: HTMLElement;

    constructor(private columnDescription: IColumnDescription,
                private displayName: string,
                private schema: SchemaClass) {
        super("Compare", "Compare values");
        this.explanation = this.addText("Value == row[" + displayName + "]");

        if (columnDescription == null) {
            const cols = this.schema.columnNames.map((c) => this.schema.displayName(c));
            if (cols.length === 0)
                return;
            const col = this.addSelectField("column", "Column", cols, null, "Column that is filtered");
            col.onchange = () => this.selectionChanged();
        }
        const val = this.addTextField("value", "Value", FieldKind.String, "?", "Value to compare");
        val.onchange = () => this.selectionChanged();
        const op = this.addSelectField("operation", "Compare", ["==", "!=", "<", ">", "<=", ">="], "<",
            "Operation that is used to compare; the value is used at the right in the comparison.");
        op.onchange = () => this.selectionChanged();
        this.setCacheTitle("ComparisonFilterDialog");
    }

    protected getColName(): string {
        if (this.columnDescription == null)
            return this.getFieldValue("column");
        return this.schema.displayName(this.columnDescription.name);
    }

    protected selectionChanged(): void {
        this.explanation.textContent = this.getFieldValue("value") + " " +
            this.getFieldValue("operation") +
            " row[" + this.getColName() + "]";
    }

    public getFilter(): ComparisonFilterDescription {
        let value: string = this.getFieldValue("value");
        if (this.columnDescription == null) {
            const colName = this.getFieldValue("column");
            this.columnDescription = this.schema.findByDisplayName(colName);
        }
        if (this.columnDescription.kind === "Date") {
            const date = new Date(value);
            value = Converters.doubleFromDate(date).toString();
        }
        const comparison = this.getFieldValue("operation") as Comparison;
        return {
            column: this.columnDescription.name,
            compareValue: value,
            comparison,
        };
    }
}

class CountReceiver extends OnCompleteReceiver<HLogLog> {
    constructor(page: FullPage, operation: ICancellable<HLogLog>,
                protected colName: string) {
        super(page, operation, "HyperLogLog");
    }

    public run(data: HLogLog): void {
        const timeInMs = this.elapsedMilliseconds();
        this.page.reportError("Distinct values in column \'" +
            this.colName + "\' " + SpecialChars.approx + String(data.distinctItemCount) + "\n" +
            "Operation took " + significantDigits(timeInMs / 1000) + " seconds");
    }
}

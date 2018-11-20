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
    allContentsKind,
    asContentsKind,
    ColumnSortOrientation,
    Comparison,
    ComparisonFilterDescription,
    ContentsKind,
    ConvertColumnInfo,
    CreateColumnInfo,
    HLogLog,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
    StringFilterDescription,
    StringRowFilterDescription
} from "../javaBridge";
import {OnCompleteReceiver} from "../rpc";
import {SchemaClass} from "../schemaClass";
import {BaseRenderer, BigTableView} from "../tableTarget";
import {Dialog, FieldKind} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {SubMenu, TopMenuItem} from "../ui/menu";
import {HtmlString, SpecialChars, ViewKind} from "../ui/ui";
import {
    cloneToSet,
    Converters,
    convertToStringFormat,
    ICancellable,
    mapToArray,
    significantDigits,
} from "../util";
import {HeavyHittersReceiver, HeavyHittersView} from "./heavyHittersView";
import {DataRangesCollector} from "./dataRangesCollectors";
import {TableOperationCompleted, TableView} from "./tableView";
import {HistogramDialog} from "./histogramView";
import {ErrorReporter} from "../ui/errReporter";

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
            this.page.reportError("Select only 1 column to rename");
            return;
        }
        const colName = cols[0];
        const dialog = new Dialog("Rename column",
            "Choose a new name for column " + this.schema.displayName(colName));
        const name = dialog.addTextField("name", "New name",
            FieldKind.String, this.schema.displayName(colName), "New name to use for column");
        name.required = true;
        dialog.setAction(() => this.doRenameColumn(colName, dialog.getFieldValue("name")));
        dialog.show();
    }

    public doRenameColumn(from: string, to: string): void {
        this.schema = this.schema.clone();
        if (!this.schema.changeDisplayName(from, to)) {
            this.page.reportError("Cannot rename column to " + to + " since the name is already used.");
            return;
        }
        this.resize();
    }

    public saveAsOrc(schema: SchemaClass): void {
        const dialog = new Dialog("Save as ORC files",
            "Describe the set of ORC files where data will be saved.");
        const folder = dialog.addTextField("folderName", "Folder", FieldKind.String, "/",
            "All ORC files will be written to this folder on each of the remote machines.");
        folder.required = true;
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
            const folderName = dialog.getFieldValue("folderName");
            const rr = this.createStreamingRpcRequest<boolean>("saveAsOrc", {
                folderName,
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
        const name = dialog.addTextField(
            "outColName", "Column name", FieldKind.String, null, "Name to use for the generated column.");
        name.required = true;
        dialog.addSelectField(
            "outColKind", "Data type", allContentsKind, "String",
            "Type of data in the generated column.");
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
        const newPage = this.dataset.newPage(new PageTitle("New column " + col), this.page);
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
        const rr = this.createDataRangesRequest([cd], this.page, "Histogram");
        rr.invoke(new DataRangesCollector(
            this, this.page, rr, this.schema, [0], [cd], null,
            { chartKind: "Histogram", relative: false, exact: false, reusePage: false }));
    }

    protected histogram2D(cds: IColumnDescription[]): void {
        const rr = this.createDataRangesRequest(cds, this.page, "2DHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: "2DHistogram", exact: false
        }));
    }

    protected trellis2D(cds: IColumnDescription[]): void {
        const rr = this.createDataRangesRequest(cds, this.page, "TrellisHistogram");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: "TrellisHistogram", exact: false
        }));
    }

    protected trellis3D(cds: IColumnDescription[], heatmap: boolean): void {
        let chartKind: ViewKind;
        if (heatmap)
            chartKind = "TrellisHeatmap";
        else {
            chartKind = "Trellis2DHistogram";
        }
        const rr = this.createDataRangesRequest(cds, this.page, chartKind);
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0, 0], cds, null, {
            reusePage: false, relative: false,
            chartKind: chartKind, exact: false
        }));
    }

    protected histogram(columns: string[]): void {
        const cds = this.schema.getDescriptions(columns);
        if (cds.length === 1) {
            this.histogram1D(cds[0]);
        } else {
            this.histogram2D(cds);
        }
    }

    protected trellis(columns: string[], heatmap: boolean): void {
        const cds = this.schema.getDescriptions(columns);
        if (cds.length === 2) {
            console.assert(!heatmap);
            this.trellis2D(cds);
        } else {
            this.trellis3D(cds, heatmap);
        }
    }

    protected heatmap(columns: string[]): void {
        const cds = this.schema.getDescriptions(columns);
        const rr = this.createDataRangesRequest(cds, this.page, "Heatmap");
        rr.invoke(new DataRangesCollector(this, this.page, rr, this.schema,
            [0, 0], cds, null, {
            reusePage: false,
            relative: false,
            chartKind: "Heatmap",
            exact: true
        }));
    }

    protected heatmapSelected(): void {
        if (this.getSelectedColCount() !== 2) {
            this.page.reportError("Must select 2 columns for heatmap");
            return;
        }
        this.heatmap(this.getSelectedColNames());
    }

    protected histogramSelected(): void {
        if (this.getSelectedColCount() < 1 || this.getSelectedColCount() > 2) {
            this.page.reportError("Must select 1 or 2 columns for histogram");
            return;
        }
        this.histogram(this.getSelectedColNames());
    }

    protected trellisSelected(heatmap: boolean): void {
        if (this.getSelectedColCount() < 2 || this.getSelectedColCount() > 3) {
            this.page.reportError("Must select 1 or 2 columns for Trellis polots");
            return;
        }
        this.trellis(this.getSelectedColNames(), heatmap);
    }

    public twoDHistogramMenu(heatmap: boolean): void {
        if (this.schema.length < 2) {
            this.page.reportError("Could not find two columns that can be charted.");
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
                    this.page.reportError("The two columns must be distinct");
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

    public trellisMenu(twoD: boolean, heatmap: boolean): void {
        const count = twoD ? 2 : 3;
        if (this.schema.length < count) {
            this.page.reportError("Could not find enough columns that can be charted.");
            return;
        }

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
                    this.page.reportError("The columns must be distinct");
                    return;
                }
                let col2 = null;
                const colNames = [col0, col1];

                if (count === 3) {
                    const c2 = dia.getFieldValue("columnName2");
                    col2 = this.schema.fromDisplayName(c2);
                    if (col0 === col2 || col1 === col2) {
                        this.page.reportError("The columns must be distinct");
                        return;
                    }
                    colNames.push(col2);
                }
                this.trellis(colNames, heatmap);
            },
        );
        dia.show();
    }

    protected hLogLog(): void {
        if (this.getSelectedColCount() !== 1) {
            this.page.reportError("Only one column must be selected");
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
                { text: "Trellis histograms...", action: () => this.trellisMenu(true, false),
                    help: "Draw a Trellis plot of histograms."},
                { text: "Trellis 2D histograms...", action: () => this.trellisMenu(false, false),
                    help: "Draw a Trellis plot of 2D histograms."},
                { text: "Trellis 2D heatmaps...", action: () => this.trellisMenu(false, true),
                    help: "Draw a Trellis plot of 3D histograms."},
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
            const rowFilter = ef.getFilter();
            const strFilter = rowFilter.stringFilterDescription;
            const desc = this.schema.find(rowFilter.colName);
            const o = order.clone();
            const so: ColumnSortOrientation = {
                columnDescription: desc,
                isAscending: true,
            };
            o.addColumn(so);
            const rr = this.createFilterEqualityRequest(rowFilter);
            let title = "Filtered: " + rowFilter.colName;
            if ((strFilter.asSubString || strFilter.asRegEx) && !strFilter.complement)
                title += " contains ";
            else if ((strFilter.asSubString || strFilter.asRegEx) && strFilter.complement)
                title += " does not contain ";
            else if (!(strFilter.asSubString || strFilter.asRegEx) && !strFilter.complement)
                title += " equals ";
            else if (!(strFilter.asSubString || strFilter.asRegEx) && strFilter.complement)
                title += " does not equal ";
            title += convertToStringFormat(strFilter.compareValue, desc.kind);
            const newPage = this.dataset.newPage(new PageTitle(title), this.page);
            rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema, o, tableRowsDesired));
        });
        ef.show();
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param displayName       Column name.  If null the user will select the column.
     * @param order             Current record ordering.
     * @param tableRowsDesired  Number of table rows to display.
     */
    protected showCompareDialog(
        displayName: string, order: RecordOrder, tableRowsDesired: number): void {
        const cd = this.schema.findByDisplayName(displayName);
        const cfd = new ComparisonFilterDialog(cd, displayName, this.schema, this.page.getErrorReporter());
        cfd.setAction(() => this.runComparisonFilter(cfd.getFilter(), order, tableRowsDesired));
        cfd.show();
    }

    protected runComparisonFilter(
        filter: ComparisonFilterDescription | null, order: RecordOrder, tableRowsDesired: number): void {
        if (filter == null)
            // Some error occurred
            return;

        const kind = filter.column.kind;
        const so: ColumnSortOrientation = {
            columnDescription: filter.column, isAscending: true,
        };
        const o = order.clone();
        o.addColumn(so);

        const rr = this.createFilterComparisonRequest(filter);
        const value = kindIsString(kind) ? filter.stringValue : filter.doubleValue;
        const title = "Filtered: " + convertToStringFormat(value, kind) +
                    " " + filter.comparison + " " + this.schema.displayName(filter.column.name);
        const newPage = this.dataset.newPage(new PageTitle(title), this.page);
        rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema, o, tableRowsDesired));
    }

    protected runHeavyHitters(percent: number): void {
        if (percent == null || percent < HeavyHittersView.min || percent > 100) {
            this.page.reportError("Percentage must be between " + HeavyHittersView.min.toString() + " and 100");
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
            columnsShown, percent, this.rowCount, HeavyHittersView.switchToMG);
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
        const perc = d.addTextField("percent", "Threshold (%)", FieldKind.Double, "1",
            "All values that appear in the dataset with a frequency above this value (as a percent) " +
            "will be considered frequent elements.  Must be a number between " + HeavyHittersView.minString +
            " and 100%.");
        perc.min = HeavyHittersView.minString;
        perc.max = "100";
        perc.required = true;
        d.setAction(() => {
            const amount = d.getFieldValueAsNumber("percent");
            if (amount != null)
                this.runHeavyHitters(amount);
        });
        d.setCacheTitle("HeavyHittersDialog");
        d.show();
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
        this.addBooleanField("asSubString", "Match substrings", false, "Select "
            + "checkbox to allow matching the search query as a substring");
        this.addBooleanField("asRegEx", "Treat as regular expression", false, "Select "
            + "checkbox to interpret the search query as a regular expression");
        this.addBooleanField("caseSensitive", "Case Sensitive", false, "Select checkbox "
            + "to do a case sensitive search");
        this.addBooleanField("complement", "Exclude matches", false, "Select checkbox to "
            + "filter out all matches");
        this.setCacheTitle("EqualityFilterDialog");
    }

    public getFilter(): StringRowFilterDescription {
        let textQuery: string = this.getFieldValue("query");
        if (this.columnDescription == null) {
            const colName = this.getFieldValue("column");
            this.columnDescription = this.schema.find(this.schema.fromDisplayName(colName));
        }
        if (this.columnDescription.kind === "Date") {
            const date = new Date(textQuery);
            textQuery = Converters.doubleFromDate(date).toString();
        }
        const asSubString = this.getBooleanValue("asSubString");
        const asRegEx = this.getBooleanValue("asRegEx");
        const caseSensitive = this.getBooleanValue("caseSensitive");
        const complement = this.getBooleanValue("complement");
        const stringFilterDescription: StringFilterDescription = {
            compareValue: textQuery,
            asSubString: asSubString,
            asRegEx: asRegEx,
            caseSensitive: caseSensitive,
            complement: complement,
        };
        return {
            colName: this.columnDescription.name,
            stringFilterDescription: stringFilterDescription,
        };
    }
}

class ComparisonFilterDialog extends Dialog {
    private explanation: HTMLElement;

    constructor(private columnDescription: IColumnDescription | null,
                private displayName: string,
                private schema: SchemaClass,
                private reporter: ErrorReporter) {
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
        val.required = true;
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

    public getFilter(): ComparisonFilterDescription | null {
        const value: string = this.getFieldValue("value");
        let doubleValue: number = null;

        if (this.columnDescription == null) {
            const colName = this.getFieldValue("column");
            this.columnDescription = this.schema.findByDisplayName(colName);
        }
        if (this.columnDescription.kind === "Date") {
            const date = new Date(value);
            if (date == null) {
                this.reporter.reportError("Could not parse '" + value + "' as a date");
                return null;
            }
            doubleValue = Converters.doubleFromDate(date);
        } else if (!kindIsString(this.columnDescription.kind)) {
            doubleValue = parseFloat(value);
            if (doubleValue == null) {
                this.reporter.reportError("Could not parse '" + value + "' as a number");
                return null;
            }
        }
        const comparison = this.getFieldValue("operation") as Comparison;
        return {
            column: this.columnDescription,
            stringValue: kindIsString(this.columnDescription.kind) ? value : null,
            doubleValue: doubleValue,
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

/**
 * A dialog to find out information about how to perform the conversion of the data in a column.
 */
export class ConverterDialog extends Dialog {
    // noinspection TypeScriptFieldCanBeMadeReadonly
    private columnNameFixed: boolean = false;

    constructor(protected readonly columnName: string,
                protected readonly allColumns: string[]) {
        super("Convert column", "Creates a new column by converting the data in an existing column to a new type.");
        const cn = this.addSelectField("columnName", "Column: ", allColumns, columnName,
            "Column whose type is converted");
        const nk = this.addSelectField("newKind", "Convert to: ",
            allContentsKind, null,
            "Type of data for the converted column.");
        const nn = this.addTextField("newColumnName", "New column name: ", FieldKind.String, null,
            "A name for the new column.  The name must be different from all other column names.");
        nn.required = true;
        cn.onchange = () => this.generateColumnName();
        nk.onchange = () => this.generateColumnName();
        // If the user types a column name don't attempt to change it
        nn.onchange = () => this.columnNameFixed = true;
        this.setCacheTitle("ConverterDialog");
        this.setFieldValue("columnName", columnName);
        this.generateColumnName();
    }

    private generateColumnName(): void {
        if (this.columnNameFixed)
            return;
        const cn = this.getFieldValue("columnName");
        const suffix = " (" + this.getFieldValue("newKind") + ")";
        let nn = cn + suffix;
        if (this.allColumns.indexOf(nn) >= 0) {
            let counter = 0;
            while (this.allColumns.indexOf(nn) >= 0) {
                nn = cn + counter.toString() + suffix;
                counter++;
            }
        }
        this.setFieldValue("newColumnName", nn);
    }
}

/**
 * This class handles type conversions on columns (e.g. String to Integer).
 */
export class ColumnConverter  {
    private readonly columnIndex: number;  // index of original column in schema

    constructor(private columnName: string,
                private newKind: ContentsKind,
                private newColumnName: string,
                private table: TableView,
                private order: RecordOrder,
                private page: FullPage) {
        this.columnIndex = this.table.schema.columnIndex(this.columnName);
    }

    public run(): void {
        if (this.table.schema.columnIndex(this.newColumnName) >= 0) {
            this.page.reportError(`Column name ${this.newColumnName} already exists in table.`);
            return;
        }

        const args: ConvertColumnInfo = {
            colName: this.columnName,
            newColName: this.newColumnName,
            newKind: this.newKind,
            columnIndex: this.columnIndex,
        };
        const rr = this.table.createStreamingRpcRequest<string>("convertColumnMap", args);
        const cd: IColumnDescription = {
            kind: this.newKind,
            name: this.newColumnName,
        };
        const schema = this.table.schema.append(cd);
        const o = this.order.clone();
        o.addColumn({columnDescription: cd, isAscending: true});
        rr.invoke(new TableOperationCompleted(this.page, rr, this.table.rowCount, schema,
            o, this.table.tableRowsDesired));
    }
}

export class CountSketchReceiver extends BaseRenderer {
    private csThreshold: number = 0.01;
    public constructor(page: FullPage,
                       protected tv: TableView,
                       operation: ICancellable<RemoteObjectId>,
                       protected rowCount: number,
                       protected schema: SchemaClass,
                       protected columnsShown: IColumnDescription [],
                       protected order: RecordOrder) {
        super(page, operation, "Count Sketch Receiver", tv.dataset);
    }

    public run(): void {
        super.run();
        const rr = this.tv.createExactCSRequest(this.remoteObject);
        rr.chain(this.operation);
        rr.invoke(new HeavyHittersReceiver(this.page, this.tv, this.operation,
            this.rowCount, this.schema, this.order, false, this.csThreshold, this.columnsShown, false));
    }
}

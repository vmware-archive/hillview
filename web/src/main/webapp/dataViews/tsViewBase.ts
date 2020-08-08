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
    CreateColumnJSMapInfo,
    IColumnDescription,
    kindIsString,
    RecordOrder,
    RemoteObjectId,
    StringFilterDescription,
    StringColumnFilterDescription, AggregateDescription, CountWithConfidence
} from "../javaBridge";
import {OnCompleteReceiver} from "../rpc";
import {DisplayName, SchemaClass} from "../schemaClass";
import {BigTableView} from "../modules";
import {Dialog, FieldKind, saveAs} from "../ui/dialog";
import {FullPage, PageTitle} from "../ui/fullPage";
import {SubMenu, TopMenuItem} from "../ui/menu";
import {SpecialChars, ViewKind} from "../ui/ui";
import {
    cloneToSet,
    Converters,
    ICancellable,
    significantDigits,
} from "../util";
import {HeavyHittersReceiver, HeavyHittersView} from "./heavyHittersView";
import {DataRangesReceiver} from "./dataRangesReceiver";
import {TableOperationCompleted} from "../modules";
import {ErrorReporter} from "../ui/errReporter";

/**
 * A base class for TableView and SchemaView.
 */
export abstract class TSViewBase extends BigTableView {
    protected defaultProvenance: string;

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

    /**
     * Convert the data in a column to a different column kind.
     */
    public convert(origDisplayName: DisplayName, order: RecordOrder | null,
                   rowsDesired: number, aggregates: AggregateDescription[] | null): void {
        const dialog = new ConverterDialog(origDisplayName, this.schema);
        dialog.setAction(
            () => {
                const displayName = dialog.getColumnName("columnName");
                const columnIndex = this.schema.columnIndex(this.schema.fromDisplayName(displayName));
                const kindStr = dialog.getFieldValue("newKind");
                const kind: ContentsKind = asContentsKind(kindStr);
                const keep = dialog.getBooleanValue("keep");
                const newColName = keep ?
                    dialog.getFieldValue("newColumnName") :
                    this.schema.uniqueColumnName(displayName.displayName);

                if (this.schema.columnIndex(newColName) >= 0) {
                    this.page.reportError(`Column name ${newColName} already exists in table.`);
                    return;
                }

                const args: ConvertColumnInfo = {
                    colName: this.schema.findByDisplayName(displayName).name,
                    newColName: newColName,
                    newKind: kind,
                    columnIndex: columnIndex,
                };
                const rr = this.createStreamingRpcRequest<string>("convertColumn", args);
                const cd: IColumnDescription = {
                    kind: kind,
                    name: newColName,
                };
                let schema = this.schema.insert(cd, columnIndex);
                const o = order != null ? order.clone() : null;
                if (o != null)
                    o.addColumn({columnDescription: cd, isAscending: true});
                if (!keep) {
                    const initial = schema.length;
                    schema = schema.filter((c) => !schema.displayName(c.name).equals(displayName));
                    console.assert(schema.length < initial);
                    const ok = schema.changeDisplayName(new DisplayName(newColName), displayName.displayName);
                    console.assert(ok);
                }
                rr.invoke(new TableOperationCompleted(this.page, rr, this.rowCount, schema,
                    o, rowsDesired, aggregates));
            });
        dialog.show();
    }

    protected exportSchema(): void {
        saveAs("schema.json", JSON.stringify(this.schema.schema));
    }

    public renameColumn(): void {
        const cols = this.getSelectedColNames();
        if (cols.length !== 1) {
            this.page.reportError("Select only 1 column to rename");
            return;
        }
        const colName = cols[0];
        const displayName = this.schema.displayName(colName);
        const dialog = new Dialog("Rename column", "Choose a new name for column " + displayName);
        const name = dialog.addTextField("name", "New name",
            FieldKind.String, displayName.displayName, "New name to use for column");
        name.required = true;
        dialog.setAction(() => this.doRenameColumn(displayName, dialog.getFieldValue("name")));
        dialog.show();
    }

    public doRenameColumn(fromDisplayName: DisplayName, to: string): void {
        this.schema = this.schema.clone();
        if (!this.schema.changeDisplayName(fromDisplayName, to)) {
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
                renameMap: schema.getRenameVector(),
            });
            const renderer = new SaveReceiver(this.page, rr);
            rr.invoke(renderer);
        });
        dialog.show();
    }

    public createJSColumnDialog(order: RecordOrder | null, tableRowsDesired: number,
                                aggregates: AggregateDescription[] | null): void {
        const dialog = new Dialog(
            "Create new column", "Specify a JavaScript function which computes the values in a new column.");
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
        dialog.setCacheTitle("CreateJSDialog");
        dialog.setAction(() => this.createJSColumn(dialog, order, tableRowsDesired, aggregates));
        dialog.show();
    }

    private createJSColumn(dialog: Dialog, order: RecordOrder | null,
                           tableRowsDesired: number, aggregates: AggregateDescription[] | null): void {
        const col = dialog.getFieldValue("outColName");
        if (this.schema.find(col) != null) {
            this.page.reportError("Column " + col + " already exists");
            return;
        }
        const kind = dialog.getFieldValue("outColKind");
        const fun = "function map(row) {" + dialog.getFieldValue("function") + "}";
        const selColumns = cloneToSet(this.getSelectedColNames());
        const subSchema = this.schema.filter((c) => selColumns.has(c.name));
        const arg: CreateColumnJSMapInfo = {
            jsFunction: fun,
            outputColumn: col,
            outputKind: asContentsKind(kind),
            schema: subSchema.schema,
            renameMap: subSchema.getRenameVector(),
        };
        const rr = this.createJSCreateColumnRequest(arg);
        const cd: IColumnDescription = {
            kind: arg.outputKind,
            name: col,
        };
        const schema = this.schema.append(cd);
        const o = order != null ? order.clone() : null;
        if (o != null)
            o.addColumn({columnDescription: cd, isAscending: true});

        const rec = new TableOperationCompleted(
            this.page, rr, this.rowCount, schema, o, tableRowsDesired, aggregates);
        rr.invoke(rec);
    }

    protected chart(cds: IColumnDescription[], chartKind: ViewKind): void {
        const exact = this.isPrivate(); // If private, do not sample
        const cols = chartKind === "Heatmap" ? cds.slice(0,2) : cds;
        const rr = this.createDataQuantilesRequest(cols, this.page, chartKind);
        const buckets = cols.map(_ => 0);
        rr.invoke(new DataRangesReceiver(this, this.page, rr, this.schema,
            buckets, cds, null, this.defaultProvenance, {
            reusePage: false, relative: false,
            chartKind: chartKind, exact: exact
        }));
    }

    public two2ChartMenu(viewKind: ViewKind): void {
        if (this.schema.length < 2) {
            this.page.reportError("Could not find two columns that can be charted.");
            return;
        }

        const allColumns = this.schema.allDisplayNames();
        const dia = new Dialog(viewKind,
                        "Display a " + viewKind + " of the data in two columns");
        dia.addColumnSelectField("columnName0", "First column", allColumns, allColumns[0],
                    "First column (X axis)");
        dia.addColumnSelectField("columnName1", "Second column", allColumns, allColumns[1],
                    "Second column " + (viewKind === "2DHistogram" ? "(Y axis)" : "(color)"));
        dia.setAction(
            () => {
                const c0 = dia.getColumnName("columnName0");
                const col0 = this.schema.fromDisplayName(c0);
                const c1 = dia.getColumnName("columnName1");
                const col1 = this.schema.fromDisplayName(c1);
                if (col0 === col1) {
                    this.page.reportError("The two columns must be distinct");
                    return;
                }
                const colDesc = this.schema.getDescriptions([col0, col1]);
                this.chart(colDesc, viewKind);
            },
        );
        dia.show();
    }

    public trellisMenu(chartKind: ViewKind): void {
        const count = chartKind == "TrellisHistogram" ? 2 : 3;
        const allColumns = this.schema.allDisplayNames();
        const dia = new Dialog(chartKind,
            "Display a " + chartKind);
        dia.addColumnSelectField("columnName0", "First column", allColumns, allColumns[0],
            "First column (X axis)");
        const secCol = count === 2 ? "Column to group by" : "Second column ";
        dia.addColumnSelectField("columnName1", secCol, allColumns, allColumns[1], secCol);
        if (count === 3)
            dia.addColumnSelectField("columnName2", "Column to group by", allColumns, allColumns[2],
                "Column to group by");

        dia.setAction(
            () => {
                const c0 = dia.getColumnName("columnName0");
                const col0 = this.schema.fromDisplayName(c0);
                const c1 = dia.getColumnName("columnName1");
                const col1 = this.schema.fromDisplayName(c1);
                if (col0 === col1) {
                    this.page.reportError("The columns must be distinct");
                    return;
                }
                let col2 = null;
                const colNames = [col0, col1];

                if (count === 3) {
                    const c2 = dia.getColumnName("columnName2");
                    col2 = this.schema.fromDisplayName(c2);
                    if (col0 === col2 || col1 === col2) {
                        this.page.reportError("The columns must be distinct");
                        return;
                    }
                    colNames.push(col2);
                }

                const columnDescriptions = this.schema.getDescriptions(colNames);
                this.chart(columnDescriptions, chartKind);
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
        const dia = new HistogramDialog(this.schema.allDisplayNames());
        dia.setAction(
            () => {
                const col = this.schema.fromDisplayName(dia.getColumn());
                const cds = this.schema.getDescriptions([col]);
                this.chart(cds, "Histogram");
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
                { text: "2D Histogram...", action: () => this.two2ChartMenu("2DHistogram"),
                    help: "Draw a histogram of the data in two columns."},
                { text: "Quartile vector plot...", action: () => this.two2ChartMenu("QuartileVector"),
                    help: "Draw a vector of quartiles."},
                { text: "Heatmap...", action: () => this.two2ChartMenu("Heatmap"),
                    help: "Draw a heatmap of the data in two columns."},
                { text: "Trellis histograms...", action: () => this.trellisMenu("TrellisHistogram"),
                    help: "Draw a Trellis plot of histograms."},
                { text: "Trellis 2D histograms...", action: () => this.trellisMenu("Trellis2DHistogram"),
                    help: "Draw a Trellis plot of 2D histograms."},
                { text: "Trellis heatmaps...", action: () => this.trellisMenu("TrellisHeatmap"),
                    help: "Draw a Trellis plot of heatmaps."},
                { text: "Trellis quartiles...", action: () => this.trellisMenu("TrellisQuartiles"),
                    help: "Draw a Trellis plot of quartile vectors."},
            ]),
        };
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param displayColName     Display column name.  If null the user will select the column.
     * @param order  Current record ordering; if null the data will be displayed in a schema view.
     * @param tableRowsDesired  Number of table rows to display.
     * @param aggregates        Aggregations that should be computed.
     */
    protected showFilterDialog(
        displayColName: DisplayName, order: RecordOrder | null, tableRowsDesired: number,
        aggregates: AggregateDescription[] | null): void {
        const cd = this.schema.findByDisplayName(displayColName);
        const ef = new FilterDialog(cd, this.schema);
        ef.setAction(() => {
            const rowFilter = ef.getFilter();
            const strFilter = rowFilter.stringFilterDescription;
            const desc = this.schema.find(rowFilter.colName);
            let o = null;
            if (order != null) {
                o = order.clone();
                const so: ColumnSortOrientation = {
                    columnDescription: desc,
                    isAscending: true,
                };
                o.addColumn(so);
            }
            const rr = this.createFilterColumnRequest(rowFilter);
            let provenance = "Filtered " + rowFilter.colName + Converters.stringFilterDescription(strFilter);
            const newPage = this.dataset.newPage(new PageTitle(this.page.title.format, provenance), this.page);
            rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema,
                o, tableRowsDesired, aggregates));
        });
        ef.show();
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param displayName       Column display name.  If null the user will select the column.
     * @param order             Current record ordering.
     * @param tableRowsDesired  Number of table rows to display.
     * @param aggregates        Aggregations that should be computed.
     */
    protected showCompareDialog(
        displayName: DisplayName, order: RecordOrder | null, tableRowsDesired: number,
        aggregates: AggregateDescription[] | null): void {
        const cfd = new ComparisonFilterDialog(displayName, this.schema, this.page.getErrorReporter());
        cfd.setAction(() => this.runComparisonFilter(
            cfd.getFilter(), order, tableRowsDesired, aggregates));
        cfd.show();
    }

    protected runComparisonFilter(
        filter: ComparisonFilterDescription | null, order: RecordOrder | null,
        tableRowsDesired: number, aggregates: AggregateDescription[] | null): void {
        if (filter == null)
            // Some error occurred
            return;

        const so: ColumnSortOrientation = {
            columnDescription: filter.column, isAscending: true,
        };
        let o = null;
        if (order != null) {
            o = order.clone();
            if (!o.find(filter.column.name))
                o.addColumn(so);
        }

        const rr = this.createFilterComparisonRequest(filter);
        const provenance = Converters.comparisonFilterDescription(filter);
        const newPage = this.dataset.newPage(new PageTitle(this.page.title.format, provenance), this.page);
        rr.invoke(new TableOperationCompleted(newPage, rr, this.rowCount, this.schema,
            o, tableRowsDesired, aggregates));
    }

    protected runHeavyHitters(percent: number): void {
        if (percent == null || percent < HeavyHittersView.min || percent > 100) {
            this.page.reportError("Percentage must be between " + HeavyHittersView.min.toString() + " and 100");
            return;
        }
        const isApprox: boolean = true;
        const columnsShown: IColumnDescription[] = [];
        this.getSelectedColNames().forEach((v) => {
            const colDesc = this.schema.find(v);
            columnsShown.push(colDesc);
        });
        const rr = this.createHeavyHittersRequest(
            columnsShown, percent, this.rowCount, HeavyHittersView.switchToMG);
        rr.invoke(new HeavyHittersReceiver(
            this.getPage(), this, rr, this.rowCount, this.schema,
            isApprox, percent, columnsShown, false));
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

class FilterDialog extends Dialog {
    /**
     * Create a FilterDialog
     * @param columnDescription  Display name of the column that is being filtered.
     * @param schema             Schema of the data.
     */
    constructor(private columnDescription: IColumnDescription, private schema: SchemaClass) {
        super("Filter" + (columnDescription == null ? "" : (" " + columnDescription.name)),
            "Eliminates data from a column according to its value.");
        if (columnDescription == null) {
            const cols = this.schema.allDisplayNames();
            if (cols.length === 0)
                return;
            this.addColumnSelectField("column", "Column", cols, null, "Column that is filtered");
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
        this.setCacheTitle("FilterDialog");
    }

    public getFilter(): StringColumnFilterDescription {
        const textQuery: string = this.getFieldValue("query");
        if (this.columnDescription == null) {
            const colName = this.getColumnName("column");
            this.columnDescription = this.schema.find(this.schema.fromDisplayName(colName));
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

    constructor(private displayName: DisplayName | null,
                private schema: SchemaClass,
                private reporter: ErrorReporter) {
        super("Compare " + (displayName != null ? displayName.toString() : ""),
            "Compare values");
        this.explanation = this.addText("Value == row[" +
            (displayName != null ? displayName.toString() : "?") + "]");

        if (displayName == null) {
            const cols = this.schema.allDisplayNames();
            if (cols.length === 0)
                return;
            const col = this.addColumnSelectField("column", "Column", cols, null, "Column that is filtered");
            col.onchange = () => this.selectionChanged();
        }
        const val = this.addTextField("value", "Value", FieldKind.String, "?", "Value to compare");
        val.onchange = () => this.selectionChanged();
        val.required = true;
        const op = this.addSelectField("operation", "Compare", ["==", "!=", "<", ">", "<=", ">="], "<",
            "Operation that is used to compare; the value is used at the right in the comparison.");
        op.onchange = () => this.selectionChanged();
        this.setCacheTitle("ComparisonFilterDialog");
        this.selectionChanged();
    }

    protected getColName(): DisplayName {
        if (this.displayName == null)
            return this.getColumnName("column");
        return this.displayName;
    }

    protected selectionChanged(): void {
        this.explanation.textContent = this.getFieldValue("value") + " " +
            this.getFieldValue("operation") +
            " row[" + this.getColName().toString() + "]";
    }

    public getFilter(): ComparisonFilterDescription | null {
        const value: string = this.getFieldValue("value");
        let doubleValue: number = null;
        let endValue: number = null;

        let colSelected;
        if (this.displayName == null) {
            colSelected = this.getColumnName("column");
        } else {
            colSelected = this.displayName;
        }

        const columnDescription = this.schema.findByDisplayName(colSelected);
        if (columnDescription.kind === "Date") {
            const date = new Date(value);
            if (date == null) {
                this.reporter.reportError("Could not parse '" + value + "' as a date");
                return null;
            }
            doubleValue = Converters.doubleFromDate(date);
        } else if (columnDescription.kind === "LocalDate") {
            const date = new Date(value);
            if (date == null) {
                this.reporter.reportError("Could not parse '" + value + "' as a date");
                return null;
            }
            doubleValue = Converters.doubleFromLocalDate(date);
        } else if (!kindIsString(columnDescription.kind)) {
            doubleValue = parseFloat(value);
            if (doubleValue == null) {
                this.reporter.reportError("Could not parse '" + value + "' as a number");
                return null;
            }
        } else if (columnDescription.kind == "Interval") {
            const re = /\[([^:]*):([^\]]*)]/;
            const m = value.match(re);
            doubleValue = parseFloat(m[1]);
            endValue = parseFloat(m[2]);
            if (doubleValue == null || endValue == null) {
                this.reporter.reportError("Could not parse '" + value + "' as an interval");
                return null;
            }
        }
        const comparison = this.getFieldValue("operation") as Comparison;
        return {
            column: columnDescription,
            stringValue: kindIsString(columnDescription.kind) ? value : null,
            doubleValue: doubleValue,
            intervalEnd: endValue,
            comparison,
        };
    }
}

class CountReceiver extends OnCompleteReceiver<CountWithConfidence> {
    constructor(page: FullPage, operation: ICancellable<CountWithConfidence>,
                protected colName: DisplayName) {
        super(page, operation, "Estimate distinct count");
    }

    public run(data: CountWithConfidence): void {
        const timeInMs = this.elapsedMilliseconds();
        this.page.reportError("Distinct values in column \'" +
            this.colName.toString() + "\' " + SpecialChars.approx + String(data.count) + "\n" +
            "Operation took " + significantDigits(timeInMs / 1000) + " seconds");
    }
}

/**
 * A dialog to find out information about how to perform the conversion of the data in a column.
 */
export class ConverterDialog extends Dialog {
    // noinspection TypeScriptFieldCanBeMadeReadonly
    private columnNameFixed: boolean = false;

    constructor(protected readonly displayName: DisplayName,
                protected readonly schema: SchemaClass) {
        super("Convert column", "Creates a new column by converting the data in an existing column to a new type.");
        const cn = this.addColumnSelectField("columnName", "Column: ", schema.allDisplayNames(), displayName,
            "Column whose type is converted");
        const nk = this.addSelectField("newKind", "Convert to: ",
            allContentsKind, null,
            "Type of data for the converted column.");
        const check = this.addBooleanField("keep", "Keep original column", false,
            "If true the original column will be kept");
        const newNameField = this.addTextField(
            "newColumnName", "New column name: ", FieldKind.String, displayName.toString(),
            "A name for the new column.  The name must be different from all other column names.");
        this.showField("newColumnName", false);
        check.onchange = () => this.generateColumnName();
        cn.onchange = () => this.generateColumnName();
        nk.onchange = () => this.generateColumnName();
        // If the user types a column name don't attempt to change it
        newNameField.onchange = () => { this.columnNameFixed = true; };
        this.generateColumnName();
    }

    private generateColumnName(): void {
        const keep = this.getBooleanValue("keep");
        this.showField("newColumnName", keep);

        if (this.columnNameFixed)
            return;
        const cn = this.getColumnName("columnName");
        const suffix = " (" + this.getFieldValue("newKind") + ")";
        const nn = this.schema.uniqueColumnName(cn.displayName + suffix);
        this.setFieldValue("newColumnName", nn);
    }
}

export class HistogramDialog extends Dialog {
    constructor(allColumns: DisplayName[]) {
        super("1D histogram", "Display a 1D histogram of the data in a column");
        this.addColumnSelectField("columnName", "Column", allColumns, null, "Column to histogram");
    }

    public getColumn(): DisplayName {
        return this.getColumnName("columnName");
    }
}

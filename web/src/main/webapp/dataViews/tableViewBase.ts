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

import {RemoteTableObjectView} from "../tableTarget";
import {
    allContentsKind,
    asContentsKind,
    ColumnSortOrientation,
    ComparisonFilterDescription,
    CreateColumnInfo,
    EqualityFilterDescription,
    HLogLog,
    IColumnDescription,
    RangeInfo,
    RecordOrder,
    RemoteObjectId
} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {DistinctStrings} from "../distinctStrings";
import {cloneToSet, Comparison, Converters, ICancellable, significantDigits} from "../util";
import {TableOperationCompleted, TableView} from "./tableView";
import {HistogramDialog, RangeCollector} from "./histogramView";
import {Range2DCollector} from "./heatMapView";
import {TrellisPlotDialog} from "./trellisHeatMapView";
import {Histogram2DDialog} from "./histogram2DView";
import {SubMenu, TopMenuItem} from "../ui/menu";
import {SpecialChars, ViewKind} from "../ui/ui";
import {OnCompleteRenderer} from "../rpc";
import {Dialog, FieldKind} from "../ui/dialog";
import {HeavyHittersReceiver, HeavyHittersView} from "./heavyHittersView";
import {SchemaClass} from "../schemaClass";

/**
 * A base class for TableView and SchemaView
 */
export abstract class TableViewBase extends RemoteTableObjectView {
    public schema: SchemaClass;
    // Total rows in the table
    protected rowCount: number;

    protected constructor(remoteObjectId: RemoteObjectId, page: FullPage, viewKind: ViewKind) {
        super(remoteObjectId, page, viewKind);
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

    protected heatMap(): void {
        if (this.getSelectedColCount() == 3) {
            this.trellisPlot();
            return;
        }
        if (this.getSelectedColCount() != 2) {
            this.reportError("Must select exactly 2 columns for heatmap");
            return;
        }

        this.histogram(true);
    }

    reportError(s: string) {
        this.page.reportError(s);
    }

    saveAsOrc(schema: SchemaClass): void {
        let dialog = new Dialog("Save as ORC files",
            "Describe the set of ORC files where data will be saved.");
        dialog.addTextField("folderName", "Folder", FieldKind.String, "/",
            "All ORC files will be written to this folder on each of the remote machines.");
        dialog.setCacheTitle("saveAs");

        class SaveReceiver extends OnCompleteRenderer<boolean> {
            constructor (page: FullPage, operation: ICancellable) {
                super(page, operation, "Save as ORC files");
            }

            run(value: boolean): void {
                if (value)
                    this.page.reportError("Save succeeded.");
            }
        }

        dialog.setAction(() => {
            let folder = dialog.getFieldValue("folderName");
            let rr = this.createStreamingRpcRequest<boolean>("saveAsOrc", {
                folder: folder,
                schema: schema.schema
            });
            let renderer = new SaveReceiver(this.page, rr);
            rr.invoke(renderer);
        });
        dialog.show();
    }

    addColumn(order: RecordOrder): void {
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
        dialog.setAction(() => this.createColumn(dialog, order));
        dialog.show();
    }

    createColumn(dialog: Dialog, order: RecordOrder): void {
        let col = dialog.getFieldValue("outColName");
        let kind = dialog.getFieldValue("outColKind");
        let fun = "function map(row) {" + dialog.getFieldValue("function") + "}";
        let selColumns = cloneToSet(this.getSelectedColNames());
        let subSchema = this.schema.filter(c => selColumns.has(c.name));
        let arg: CreateColumnInfo = {
            jsFunction: fun,
            outputColumn: col,
            outputKind: asContentsKind(kind),
            schema: subSchema.schema
        };
        let rr = this.createCreateColumnRequest(arg);
        let newPage = this.dataset.newPage("New column " + col, this.page);
        let cd: IColumnDescription = {
            kind: arg.outputKind,
            name: col
        };
        let schema = this.schema.append(cd);
        let o = order.clone();
        o.addColumn({columnDescription: cd, isAscending: true});

        let rec = new TableOperationCompleted(
            newPage, schema, rr, o, this.dataset);
        rr.invoke(rec);
    }

    protected histogramOrHeatmap(columns: string[], heatMap: boolean): void {
        let cds: IColumnDescription[] = [];
        let catColumns: string[] = [];  // categorical columns
        columns.forEach(v => {
            let colDesc = this.schema.find(v);
            if (colDesc.kind == "String") {
                this.reportError("Histograms not supported for string columns " + colDesc.name);
                return;
            }
            if (colDesc.kind == "Category")
                catColumns.push(v);
            cds.push(colDesc);
        });

        if (cds.length != columns.length)
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
                    let ds = this.dataset.getDistinctStrings(colName);
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
                rr.invoke(new Range2DCollector(
                    cds, this.schema, distinct, this.getPage(), this, false, rr, heatMap, false, false));
            } else {
                let rr = this.createRangeRequest(rangeInfo[0]);
                rr.chain(operation);
                let title = "Histogram " + cds[0].name;
                rr.invoke(new RangeCollector(title, cds[0], this.schema, distinct[0],
                    this.getPage(), this, false, rr, false));
            }
        };

        // Get the categorical data and invoke the continuation
        this.dataset.retrieveCategoryValues(catColumns, this.getPage(), cont);
    }

    protected histogram(heatMap: boolean): void {
        if (this.getSelectedColCount() < 1 || this.getSelectedColCount() > 2) {
            this.reportError("Must select 1 or 2 columns for histogram");
            return;
        }

        this.histogramOrHeatmap(this.getSelectedColNames(), heatMap);
    }

    protected trellisPlot(): void {
        let colNames: string[] = this.getSelectedColNames();
        let dialog = new TrellisPlotDialog(colNames, this.getPage(), this.schema, this, false);
        dialog.show();
    }

    twoDHistogramMenu(heatmap: boolean): void {
        let eligible = this.schema.filter(d => d.kind != "String");
        if (eligible.length < 2) {
            this.reportError("Could not find two columns that can be charted.");
            return;
        }
        let dia = new Histogram2DDialog(eligible.columnNames, heatmap);
        dia.setAction(
            () => {
                let col0 = dia.getColumn(false);
                let col1 = dia.getColumn(true);
                if (col0 == col1) {
                    this.reportError("The two columns must be distinct");
                    return;
                }
                this.histogramOrHeatmap([col0, col1], heatmap);
            }
        );
        dia.show();
    }

    protected hLogLog(): void {
        if (this.getSelectedColCount() != 1) {
            this.reportError("Only one column must be selected");
            return;
        }
        let colName = this.getSelectedColNames()[0];
        let rr = this.createHLogLogRequest(colName);
        let rec = new CountReceiver(this.getPage(), rr, colName);
        rr.invoke(rec);
    }

    oneDHistogramMenu(): void {
        let eligible = this.schema.filter(d => d.kind != "String");
        if (eligible.length == 0) {
            this.reportError("No columns that can be histogrammed found.");
            return;
        }
        let dia = new HistogramDialog(eligible.columnNames);
        dia.setAction(
            () => {
                let col = dia.getColumn();
                this.histogramOrHeatmap([col], false);
            }
        );
        dia.show();
    }

    saveAsMenu(): TopMenuItem {
        return {
            text: "Save as", help: "Save the data to persistent storage.", subMenu: new SubMenu([
                {
                    text: "Save as ORC files...",
                    action: () => this.saveAsOrc(this.schema),
                    help: "Save the data to a set of ORC files on the remote machines."
                }
            ]),
        };
    }

    chartMenu(): TopMenuItem {
        return {
            text: "Chart", help: "Draw a chart", subMenu: new SubMenu([
                { text: "1D Histogram...", action: () => this.oneDHistogramMenu(),
                    help: "Draw a histogram of the data in one column."},
                { text: "2D Histogram....", action: () => this.twoDHistogramMenu(false),
                    help: "Draw a histogram of the data in two columns."},
                { text: "Heatmap...", action: () => this.twoDHistogramMenu(true),
                    help: "Draw a heatmap of the data in two columns."},
            ])
        };
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param {string} colName     Column name.  If null the user will select the column.
     * @param {RecordOrder} order  Current record ordering.
     */
    protected showFilterDialog(
        colName: string, order: RecordOrder): void {
        let cd = this.schema.find(colName);
        let ef = new EqualityFilterDialog(cd, this.schema);
        ef.setAction(() => {
            let filter = ef.getFilter();
            let desc = this.schema.find(filter.column);
            let o = order.clone();
            let so: ColumnSortOrientation = {
                columnDescription: desc,
                isAscending: true
            };
            o.addColumn(so);
            let rr = this.createFilterEqualityRequest(filter);
            let title = "Filtered: " + filter.column + " is " +
                (filter.complement ? "not " : "") +
                TableView.convert(filter.compareValue, desc.kind);

            let newPage = this.dataset.newPage(title, this.page);
            rr.invoke(new TableOperationCompleted(newPage, this.schema, rr, o, this.dataset));
        });
        ef.show();
    }

    /**
     * Show a dialog to compare values on the specified column.
     * @param {string} colName     Column name.  If null the user will select the column.
     * @param {RecordOrder} order  Current record ordering.
     */
    protected showCompareDialog(
        colName: string, order: RecordOrder): void {
        let cd = this.schema.find(colName);
        let cfd = new ComparisonFilterDialog(cd, this.schema);
        cfd.setAction(() => this.runComparisonFilter(cfd.getFilter(), order));
        cfd.show();
    }

    protected runComparisonFilter(filter: ComparisonFilterDescription, order: RecordOrder): void {
        let cd = this.schema.find(filter.column);
        let kind = cd.kind;
        let so: ColumnSortOrientation = {
            columnDescription: cd, isAscending: true
        };
        let o = order.clone();
        o.addColumn(so);

        let rr = this.createFilterComparisonRequest(filter);
        let title = "Filtered: " +
            TableView.convert(filter.compareValue, kind) + " " + filter.comparison + " " + filter.column;

        let newPage = this.dataset.newPage(title, this.page);
        rr.invoke(new TableOperationCompleted(newPage, this.schema, rr, o, this.dataset))
    }

    protected runHeavyHitters(percent: number) {
        if (percent == null || percent < HeavyHittersView.min || percent > 100) {
            this.reportError("Percentage must be between " + HeavyHittersView.min.toString() + " and 100");
            return;
        }
        let isApprox: boolean = true;
        let columns: IColumnDescription[] = [];
        let cso: ColumnSortOrientation[] = [];
        this.getSelectedColNames().forEach(v => {
            let colDesc = this.schema.find(v);
            columns.push(colDesc);
            cso.push({columnDescription: colDesc, isAscending: true});
        });
        let order = new RecordOrder(cso);
        let rr = this.createHeavyHittersRequest(columns, percent, this.getTotalRowCount(), HeavyHittersView.switchToMG);
        rr.invoke(new HeavyHittersReceiver(this.getPage(), this, rr, columns, order, isApprox, percent));
    }

    protected heavyHittersDialog(): void {
        let title = "Frequent Elements from ";
        let cols: string[] = this.getSelectedColNames();
        if (cols.length <= 1) {
            title += " " + cols[0];
        } else {
            title += cols.length + " columns";
        }
        let d = new Dialog(title, "Find the most frequent values in the selected columns.");
        d.addTextField("percent", "Threshold (%)", FieldKind.Double, "1",
            "All values that appear in the dataset with a frequency above this value (as a percent) " +
            "will be considered frequent elements.  Must be a number between " + HeavyHittersView.minString +
            " and 100%.");
        d.setAction(() => {
            let amount = d.getFieldValueAsNumber("percent");
            if (amount != null)
                this.runHeavyHitters(amount)
        });
        d.setCacheTitle("HeavyHittersDialog");
        d.show();
    }

    public getTotalRowCount() : number {
        return this.rowCount;
    }
}

class EqualityFilterDialog extends Dialog {
    constructor(private columnDescription: IColumnDescription, private schema: SchemaClass) {
        super("Filter", "Eliminates data from a column according to its value.");
        if (columnDescription == null) {
            let cols = this.schema.columnNames;
            if (cols.length == 0)
                return;
            this.addSelectField("column", "Column", cols, null, "Column that is filtered");
        }
        this.addTextField("query", "Find", FieldKind.String, null, "Value to search");
        this.addBooleanField("asRegEx", "Interpret as Regular Expression", false, "Select "
            + "checkbox to interpret the search query as a regular expression");
        this.addBooleanField("complement", "Exclude matches", false, "Select checkbox to "
            + "filter out all matches");
    }

    public getFilter(): EqualityFilterDescription {
        let textQuery: string = this.getFieldValue("query");
        if (this.columnDescription == null) {
            let colName = this.getFieldValue("column");
            this.columnDescription = this.schema.find(colName);
        }
        if (this.columnDescription.kind == "Date") {
            let date = new Date(textQuery);
            textQuery = Converters.doubleFromDate(date).toString();
        }
        let asRegEx = this.getBooleanValue("asRegEx");
        let complement = this.getBooleanValue("complement");
        return {
            column: this.columnDescription.name,
            compareValue: textQuery,
            complement: complement,
            asRegEx: asRegEx
        };
    }
}

class ComparisonFilterDialog extends Dialog {
    private explanation: HTMLElement;

    constructor(private columnDescription: IColumnDescription, private schema: SchemaClass) {
        super("Compare", "Compare values");
        this.explanation = this.addText("Value == row[Column]");

        if (columnDescription == null) {
            let cols = this.schema.columnNames;
            if (cols.length == 0)
                return;
            let col = this.addSelectField("column", "Column", cols, null, "Column that is filtered");
            col.onchange = () => this.selectionChanged();
        }
        let val = this.addTextField("value", "Value", FieldKind.String, "?", "Value to compare");
        val.onchange = () => this.selectionChanged();
        let op = this.addSelectField("operation", "Compare", ["==", "!=", "<", ">", "<=", ">="], "<",
            "Operation that is used to compare; the value is used at the right in the comparison.");
        op.onchange = () => this.selectionChanged();
    }

    protected selectionChanged(): void {
        this.explanation.textContent = this.getFieldValue("value") + " " +
            this.getFieldValue("operation") +
            " row[" + this.getFieldValue("column") + "]";
    }

    public getFilter(): ComparisonFilterDescription {
        let value: string = this.getFieldValue("value");
        if (this.columnDescription == null) {
            let colName = this.getFieldValue("column");
            this.columnDescription = this.schema.find(colName);
        }
        if (this.columnDescription.kind == "Date") {
            let date = new Date(value);
            value = Converters.doubleFromDate(date).toString();
        }
        let comparison = <Comparison>this.getFieldValue("operation");
        return {
            column: this.columnDescription.name,
            compareValue: value,
            comparison: comparison
        };
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

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
import {HLogLog, IColumnDescription, RangeInfo, RemoteObjectId, Schema} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {DistinctStrings} from "../distinctStrings";
import {ICancellable, significantDigits} from "../util";
import {TableView} from "./tableView";
import {CategoryCache} from "../categoryCache";
import {HistogramDialog, RangeCollector} from "./histogramView";
import {Range2DCollector} from "./heatMapView";
import {TrellisPlotDialog} from "./trellisHeatMapView";
import {Histogram2DDialog} from "./histogram2DView";
import {SubMenu, TopMenuItem} from "../ui/menu";
import {SpecialChars} from "../ui/ui";
import {OnCompleteRenderer} from "../rpc";

/**
 * A base class for TableView and SchemaView
 */
export abstract class TableViewBase extends RemoteTableObjectView {
    public schema?: Schema;

    constructor(remoteObjectId: RemoteObjectId, originalTableId: RemoteObjectId, page: FullPage) {
        super(remoteObjectId, originalTableId, page);
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

    protected histogramOrHeatmap(columns: string[], heatMap: boolean): void {
        let cds: IColumnDescription[] = [];
        let catColumns: string[] = [];  // categorical columns
        columns.forEach(v => {
            let colDesc = TableView.findColumn(this.schema, v);
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
        let eligible = TableView.dropColumns(this.schema, d => d.kind == "String");
        if (eligible.length < 2) {
            this.reportError("Could not find two columns that can be charted.");
            return;
        }
        let dia = new Histogram2DDialog(eligible.map(e => e.name), heatmap);
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
        let eligible = TableView.dropColumns(this.schema, d => d.kind == "String");
        if (eligible.length == 0) {
            this.reportError("No columns that can be histogrammed found.");
            return;
        }
        let dia = new HistogramDialog(eligible.map(e => e.name));
        dia.setAction(
            () => {
                let col = dia.getColumn();
                this.histogramOrHeatmap([col], false);
            }
        );
        dia.show();
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

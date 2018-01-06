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
import {ColumnDescription, RangeInfo, RemoteObjectId, Schema} from "../javaBridge";
import {FullPage} from "../ui/fullPage";
import {DistinctStrings} from "../distinctStrings";
import {ICancellable} from "../util";
import {TableView} from "./tableView";
import {CategoryCache} from "../categoryCache";
import {RangeCollector} from "./histogramView";
import {Range2DCollector} from "./heatMapView";
import {HeatMapArrayDialog} from "./trellisHeatMapView";

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
            this.heatMapArray();
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

    public histogram(heatMap: boolean): void {
        if (this.getSelectedColCount() < 1 || this.getSelectedColCount() > 2) {
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

        if (cds.length != this.getSelectedColCount())
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

    protected heatMapArray(): void {
        let colNames: string[] = this.getSelectedColNames();
        let dialog = new HeatMapArrayDialog(colNames, this.getPage(), this.schema, this, false);
        dialog.show();
    }

}
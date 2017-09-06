/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import {RemoteObject, Renderer} from "./rpc";
import {IDistinctStrings, DistinctStrings} from "./tableData";
import {PartialResult, ICancellable} from "./util";
import {FullPage} from "./ui";

/// This is an abstraction for the toplevel dataset, which represents
// the full table that was loaded initially.
export class TableDataSet extends RemoteObject {
    columnValues: Map<string, DistinctStrings>;
    // TODO: handle errors that can occur while retrieving DistinctStrings

    public constructor(remoteObjectId: string) {
        super(remoteObjectId);
        this.columnValues = new Map<string, DistinctStrings>();
    }

    /// Retrieves the category values for all specified column names
    /// and stores them internally in columnValues.
    /// Invokes continuation when all values are known.
    public retrieveCategoryValues(columnNames: string[],
                           page: FullPage,
                                  // The operation is the asynchronous operation
                                  // that may have retrieved the data
                           continuation: (operation: ICancellable) => void): void {
        let rr = this.createRpcRequest("uniqueStrings", columnNames);
        let renderer = new ReceiveCategory(this, continuation, page, rr);
        for (let c of columnNames) {
            if (!this.columnValues.has(c))
                renderer.addColumn(c);
        }

        if (!renderer.empty())
            rr.invoke(renderer);
        else
            continuation(rr);
    }

    public setDistinctStrings(columnName: string, values: DistinctStrings): void {
        this.columnValues.set(columnName, values);
    }

    public getDistinctStrings(columnName: string): DistinctStrings {
        return this.columnValues.get(columnName);
    }
}

// Receives a list of DistinctStrings and stores them into a TableDataSet.
class ReceiveCategory extends Renderer<IDistinctStrings[]> {
    /// List of columns whose distinct strings have to be fetched.
    protected columns: string[];
    /// Output produced: one set of distinct strings for each columns.
    protected values: IDistinctStrings[];

    public constructor(protected tds: TableDataSet,
                       protected continuation: (operation: ICancellable) => void,
                       page: FullPage,
                       operation: ICancellable) {
        super(page, operation, "Create converter");
        this.columns = [];
        this.values = null;
    }

    public addColumn(columnName: string): void {
        this.columns.push(columnName);
    }

    public empty(): boolean { return this.columns.length == 0; }

    public onNext(value: PartialResult<IDistinctStrings[]>): void {
        super.onNext(value);
        this.values = value.data;
    }

    public onCompleted(): void {
        if (this.values == null)
            return;
        if (this.columns.length != this.values.length)
            throw "Asked for " + this.columns.length + " columns, got " + this.values.length;
        for (let i=0; i < this.columns.length; i++) {
            let col = this.columns[i];
            if (this.values[i].truncated) {
                this.page.reportError("Column " + col + " has too many distinct values; it is not really a category");
            } else {
                let ds = new DistinctStrings(this.values[i]);
                this.tds.setDistinctStrings(col, ds);
            }
        }
        super.finished();
        this.continuation(this.operation);
    }
}
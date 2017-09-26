/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {Renderer} from "./rpc";
import {IDistinctStrings, DistinctStrings, RemoteTableObject} from "./tableData";
import {PartialResult, ICancellable} from "./util";
import {FullPage} from "./ui";

export class CategoryCache {
    columnValues: Map<string, DistinctStrings>;
    // TODO: handle errors that can occur while retrieving DistinctStrings

    private constructor() {
        this.columnValues = new Map<string, DistinctStrings>();
    }

    public static instance: CategoryCache = new CategoryCache();

    /// Retrieves the category values for all specified column names
    /// and stores them internally in columnValues.
    /// Invokes continuation when all values are known.
    public retrieveCategoryValues(remoteTable: RemoteTableObject,
        columnNames: string[], page: FullPage,
        // The operation is the asynchronous operation
        // that may have retrieved the data
        continuation: (operation: ICancellable) => void): void {

        let columnsToFetch: string[] = [];
        for (let c of columnNames) {
            if (!this.columnValues.has(c))
                columnsToFetch.push(c);
        }

        let rr = remoteTable.createRpcRequest("uniqueStrings", columnsToFetch);
        if (columnsToFetch.length > 0) {
            let renderer = new ReceiveCategory(this, columnsToFetch, continuation, page, rr);
            rr.invoke(renderer);
        } else {
            continuation(rr);
        }
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
    /// Output produced: one set of distinct strings for each columns.
    protected values: IDistinctStrings[];

    public constructor(
        protected cache: CategoryCache,
        protected columns: string[],
        protected continuation: (operation: ICancellable) => void,
        page: FullPage,
        operation: ICancellable) {
        super(page, operation, "Create converter");
        this.values = null;
    }

    public onNext(value: PartialResult<IDistinctStrings[]>): void {
        super.onNext(value);
        this.values = value.data;
    }

    public onCompleted(): void {
        super.onCompleted();
        if (this.values == null)
            return;
        if (this.columns.length != this.values.length)
            throw "Required " + this.columns.length + " got " + this.values.length;
        for (let i=0; i < this.values.length; i++) {
            let col = this.columns[i];
            if (this.values[i].truncated) {
                this.page.reportError("Column " + col + " has too many distinct values; it is not really a category");
            } else {
                let ds = new DistinctStrings(this.values[i]);
                this.cache.setDistinctStrings(col, ds);
            }
        }
        this.continuation(this.operation);
    }
}

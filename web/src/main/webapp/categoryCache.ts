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

import {OnCompleteRenderer} from "./rpc";
import {ICancellable} from "./util";
import {FullPage} from "./ui/fullPage";
import {DistinctStrings, IDistinctStrings} from "./distinctStrings";
import {RemoteTableObject} from "./tableTarget";
import {RemoteObjectId} from "./javaBridge";

/**
 * The CategoryCache is a singleton class that maintains a cache mapping column names
 * to sets of strings.  Each column name is from a categorical column.  (This imposes the
 * constraint that all views ever created starting from the same table will have unique column names.)
 */
export class CategoryCache {
    /**
     * For each original table id this keeps a
     * map from a column name to a set of distinct strings.
     */
    columnValues: Map<RemoteObjectId, Map<string, DistinctStrings>>;
    // TODO: handle errors that can occur while retrieving DistinctStrings

    // noinspection JSUnusedLocalSymbols
    private constructor() {
        this.columnValues = new Map<RemoteObjectId, Map<string, DistinctStrings>>();
    }

    public static instance: CategoryCache = new CategoryCache();

    /// Retrieves the category values for all specified column names
    /// and stores them internally in columnValues.
    /// Invokes continuation when all values are known.
    public retrieveCategoryValues(originalTable: RemoteTableObject,
        columnNames: string[], page: FullPage,
        // The operation is the asynchronous operation
        // that may have retrieved the data
        continuation: (operation: ICancellable) => void): void {
        if (!this.columnValues.has(originalTable.remoteObjectId))
            this.columnValues.set(originalTable.remoteObjectId, new Map<string, DistinctStrings>());
        let map = this.columnValues.get(originalTable.remoteObjectId);

        let columnsToFetch: string[] = [];
        for (let c of columnNames) {
            if (!map.has(c))
                columnsToFetch.push(c);
        }

        let rr = originalTable.createStreamingRpcRequest<IDistinctStrings[]>("uniqueStrings", columnsToFetch);
        if (columnsToFetch.length > 0) {
            let renderer = new ReceiveCategory(originalTable.originalTableId, columnsToFetch, continuation, page, rr);
            rr.invoke(renderer);
        } else {
            continuation(rr);
        }
    }

    public setDistinctStrings(originalTableId: RemoteObjectId, columnName: string, values: DistinctStrings): void {
        if (!this.columnValues.has(originalTableId))
            this.columnValues.set(originalTableId, new Map<string, DistinctStrings>());
        let map = this.columnValues.get(originalTableId);
        map.set(columnName, values);
    }

    public getDistinctStrings(originalTableId: RemoteObjectId, columnName: string): DistinctStrings {
        if (!this.columnValues.has(originalTableId))
            return null;
        let map = this.columnValues.get(originalTableId);
        return map.get(columnName);
    }
}

/**
 * Receives a list of DistinctStrings and stores them into the category cache.
 * After that it calls the supplied continuation.
  */
class ReceiveCategory extends OnCompleteRenderer<IDistinctStrings[]> {
    public constructor(
        protected originalTableId: RemoteObjectId,
        protected columns: string[],
        protected continuation: (operation: ICancellable) => void,
        page: FullPage,
        operation: ICancellable) {
        super(page, operation, "Create converter");
    }

    public run(value: IDistinctStrings[]): void {
        if (this.columns.length != value.length)
            throw "Required " + this.columns.length + " got " + value.length;
        for (let i=0; i < value.length; i++) {
            let col = this.columns[i];
            if (value[i].truncated) {
                this.page.reportError("Column " + col + " has too many distinct values; it is not really a category");
            } else {
                let ds = new DistinctStrings(value[i]);
                CategoryCache.instance.setDistinctStrings(this.originalTableId, col, ds);
            }
        }
        this.continuation(this.operation);
    }
}
